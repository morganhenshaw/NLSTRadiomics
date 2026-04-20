import SimpleITK as sitk
import subprocess
import os
import shutil
from config import DOWNLOAD_DIR

def convert_CT_to_nrrd(pid, study_uid, ct_uid):
    try:
        reader = sitk.ImageSeriesReader()
        ct_series_dir = os.path.join(DOWNLOAD_DIR, pid, study_uid, "CT_" + ct_uid)
        dicom_names = reader.GetGDCMSeriesFileNames(ct_series_dir)
        reader.SetFileNames(dicom_names)
        ct_image = reader.Execute()
        output_path = os.path.join(DOWNLOAD_DIR, pid, study_uid, "CT.nrrd")
        sitk.WriteImage(ct_image, output_path)
    except Exception as e:
        print(f"Failed to convert CT to .nrrd for patient {pid}: {study_uid}: {e}")
        raise e

def convert_SEG_to_nrrd(pid, study_uid, seg_uid):
    seg_dir = os.path.join(DOWNLOAD_DIR, pid, study_uid, "SEG_" + seg_uid)
    seg_files = [f for f in os.listdir(seg_dir) if f.endswith('.dcm')]
    if not seg_files:
        raise FileNotFoundError(f"SEG DICOM not found in {seg_dir}")
    input_dicom = os.path.join(seg_dir, seg_files[0])
    output_dir = os.path.join(DOWNLOAD_DIR, pid, study_uid)
    command = [
        "segimage2itkimage",
        "--inputDICOM", input_dicom,
        "--outputDirectory", output_dir,
        "--outputType", "nrrd",
        "--prefix", "SEG"
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        os.rename(os.path.join(output_dir, "SEG-1.nrrd"), os.path.join(output_dir, "lung.nrrd"))
        os.rename(os.path.join(output_dir, "SEG-2.nrrd"), os.path.join(output_dir, "nodules.nrrd"))
    except subprocess.CalledProcessError as e:
        print(f"SEG conversion failed for patient {pid}, study {study_uid}: {e.stderr}")
        raise e
    except FileNotFoundError as e:
        print(f"Renaming the .nrrd files failed for patient {pid}, study {study_uid}: {e}")
        raise e
    
def separate_nodules(pid, study_uid):
    nodules_path = os.path.join(DOWNLOAD_DIR, pid, study_uid, "nodules.nrrd")
    nodules_image = sitk.ReadImage(nodules_path)

    cc_filter = sitk.ConnectedComponentImageFilter()
    labeled_image = cc_filter.Execute(nodules_image > 0)
    num_nodules = cc_filter.GetObjectCount()

    label_shape_filter = sitk.LabelShapeStatisticsImageFilter()
    label_shape_filter.Execute(labeled_image)
    nodules_sizes = [label_shape_filter.GetNumberOfPixels(i) for i in range(1, num_nodules + 1)]

    for i in range(1, num_nodules + 1):
        nodule_mask = sitk.BinaryThreshold(labeled_image, lowerThreshold=i, upperThreshold=i, insideValue=1, outsideValue=0)
        output_path = os.path.join(DOWNLOAD_DIR, pid, study_uid, f"nodule_{i}.nrrd")
        sitk.WriteImage(nodule_mask, output_path)
    return nodules_sizes

def delete_series(pid, study_uid, ct_uid, seg_uid):
    study_path = os.path.join(DOWNLOAD_DIR, pid, study_uid)
    meta_json = os.path.join(study_path, "SEG-meta.json")
    #ct_path = os.path.join(study_path, "CT.nrrd")
    #lung_path = os.path.join(study_path, "lung.nrrd")
    #lung_aligned_path = os.path.join(study_path, "lung_aligned.nrrd")
    #nodules_aligned_path = os.path.join(study_path, "nodules_aligned.nrrd")
    #for path in [meta_json, ct_path, lung_path, lung_aligned_path, nodules_aligned_path]:
    #    if os.path.exists(path):
    #        os.remove(path)
    if os.path.exists(meta_json):
        os.remove(meta_json)
    ct_dir = os.path.join(study_path, "CT_" + ct_uid)
    seg_dir = os.path.join(study_path, "SEG_" + seg_uid)
    for folder in [ct_dir, seg_dir]:
        if os.path.exists(folder):
            shutil.rmtree(folder)