from __future__ import annotations

from pydicom.config import settings

from radiomics.featureextractor import RadiomicsFeatureExtractor

import collections
import json
import logging
import os
import pathlib
import threading
from itertools import chain

# import pykwalify.core
import SimpleITK as sitk

from radiomics import (
    generalinfo,
    getFeatureClasses,
    getImageTypes,
    getParameterValidationFiles,
    imageoperations,
)

logger = logging.getLogger(__name__)


class _SingletonGeometryTolerance:
    _instance = None
    _initialized = False
    _lock = threading.Lock()

    def __new__(cls, *_args, **_kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, tolerance=None):
        if not self._initialized:
            with self._lock:
                if not self._initialized:
                    self.geometryTolerance = tolerance
                    _SingletonGeometryTolerance._initialized = True

class SaveCropFeatureExtractor(RadiomicsFeatureExtractor):
    def execute(
        self,
        imageFilepath,
        maskFilepath,
        label=None,
        label_channel=None,
        voxelBased=False,
    ):
        """
        Compute radiomics signature for provide image and mask combination. It comprises of the following steps:

        1. Image and mask are loaded and normalized/resampled if necessary.
        2. Validity of ROI is checked using :py:func:`~imageoperations.checkMask`, which also computes and returns the
           bounding box.
        3. If enabled, provenance information is calculated and stored as part of the result. (Not available in voxel-based
           extraction)
        4. Shape features are calculated on a cropped (no padding) version of the original image. (Not available in
           voxel-based extraction)
        5. If enabled, resegment the mask based upon the range specified in ``resegmentRange`` (default None: resegmentation
           disabled).
        6. Other enabled feature classes are calculated using all specified image types in ``_enabledImageTypes``. Images
           are cropped to tumor mask (no padding) after application of any filter and before being passed to the feature
           class.
        7. The calculated features is returned as ``collections.OrderedDict``.

        :param imageFilepath: SimpleITK Image, or string pointing to image file location
        :param maskFilepath: SimpleITK Image, or string pointing to labelmap file location
        :param label: Integer, value of the label for which to extract features. If not specified, last specified label
            is used. Default label is 1.
        :param label_channel: Integer, index of the channel to use when maskFilepath yields a SimpleITK.Image with a vector
            pixel type. Default index is 0.
        :param voxelBased: Boolean, default False. If set to true, a voxel-based extraction is performed, segment-based
            otherwise.
        :returns: dictionary containing calculated signature ("<imageType>_<featureClass>_<featureName>":value).
            In case of segment-based extraction, value type for features is float, if voxel-based, type is SimpleITK.Image.
            Type of diagnostic features differs, but can always be represented as a string.
        """
        _settings = self.settings.copy()

        tolerance = _settings.get("geometryTolerance")
        additionalInfo = _settings.get("additionalInfo", False)
        resegmentShape = _settings.get("resegmentShape", False)

        if label is not None:
            _settings["label"] = label
        else:
            label = _settings.get("label", 1)

        if label_channel is not None:
            _settings["label_channel"] = label_channel

        if _SingletonGeometryTolerance().geometryTolerance != tolerance:
            self._setTolerance()

        if additionalInfo:
            generalInfo = generalinfo.GeneralInfo()
            generalInfo.addGeneralSettings(_settings)
            generalInfo.addEnabledImageTypes(self.enabledImagetypes)
        else:
            generalInfo = None

        if voxelBased:
            _settings["voxelBased"] = True
            kernelRadius = _settings.get("kernelRadius", 1)
            logger.info("Starting voxel based extraction")
        else:
            kernelRadius = 0

        logger.info("Calculating features with label: %d", label)
        logger.debug("Enabled images types: %s", self.enabledImagetypes)
        logger.debug("Enabled features: %s", self.enabledFeatures)
        logger.debug("Current settings: %s", _settings)

        # 1. Load the image and mask
        featureVector = collections.OrderedDict()
        image, mask = self.loadImage(
            imageFilepath, maskFilepath, generalInfo, **_settings
        )

        # 2. Check whether loaded mask contains a valid ROI for feature extraction and get bounding box
        # Raises a ValueError if the ROI is invalid
        boundingBox, correctedMask = imageoperations.checkMask(image, mask, **_settings)

        # Update the mask if it had to be resampled
        if correctedMask is not None:
            if generalInfo is not None:
                generalInfo.addMaskElements(image, correctedMask, label, "corrected")
            mask = correctedMask

        logger.debug("Image and Mask loaded and valid, starting extraction")

        # 5. Resegment the mask if enabled (parameter regsegmentMask is not None)
        resegmentedMask = None
        if _settings.get("resegmentRange", None) is not None:
            resegmentedMask = imageoperations.resegmentMask(image, mask, **_settings)

            # Recheck to see if the mask is still valid, raises a ValueError if not
            boundingBox, correctedMask = imageoperations.checkMask(
                image, resegmentedMask, **_settings
            )

            if generalInfo is not None:
                generalInfo.addMaskElements(
                    image, resegmentedMask, label, "resegmented"
                )

        # 3. Add the additional information if enabled
        if generalInfo is not None:
            featureVector.update(generalInfo.getGeneralInfo())

        # if resegmentShape is True and resegmentation has been enabled, update the mask here to also use the
        # resegmented mask for shape calculation (e.g. PET resegmentation)
        if resegmentShape and resegmentedMask is not None:
            mask = resegmentedMask

        if not voxelBased:
            # 4. If shape descriptors should be calculated, handle it separately here
            featureVector.update(
                self.computeShape(image, mask, boundingBox, **_settings)
            )

        # (Default) Only use resegemented mask for feature classes other than shape
        # can be overridden by specifying `resegmentShape` = True
        if not resegmentShape and resegmentedMask is not None:
            mask = resegmentedMask

        # 6. Calculate other enabled feature classes using enabled image types
        # Make generators for all enabled image types
        logger.debug("Creating image type iterator")
        imageGenerators = []
        for imageType, customKwargs in self.enabledImagetypes.items():
            args = _settings.copy()
            args.update(customKwargs)
            msg = f'Adding image type "{imageType}" with custom settings: {customKwargs!s}'
            logger.info(msg)
            imageGenerators = chain(
                imageGenerators,
                getattr(imageoperations, f"get{imageType}Image")(image, mask, **args),
            )

        logger.debug("Extracting features")
        # Calculate features for all (filtered) images in the generator
        for originputImage, imageTypeName, inputKwargs in imageGenerators:
            logger.info("Calculating features for %s image", imageTypeName)
            inputImage, inputMask = imageoperations.cropToTumorMask(
                originputImage, mask, boundingBox, padDistance=kernelRadius
            )

            if isinstance(maskFilepath, str):
                sitk.WriteImage(sitk.Cast(inputMask, sitk.sitkUInt8), maskFilepath[:-5] + "_cropped.nrrd", useCompression=True)
                print("Saving cropped nodule .nrrd file")

            featureVector.update(
                self.computeFeatures(
                    inputImage, inputMask, imageTypeName, **inputKwargs
                )
            )

        logger.debug("Features extracted")

        return featureVector