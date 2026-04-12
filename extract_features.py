from save_crop_feature_extractor import SaveCropFeatureExtractor
import os
from config import DOWNLOAD_DIR, FEATURES_CSV
import pandas as pd
import csv
    
def extract_features(pid, study_id):
    ct_path = os.path.join(DOWNLOAD_DIR, pid, study_id, "CT.nrrd")
    nodule_path = os.path.join(DOWNLOAD_DIR, pid, study_id, "nodule.nrrd")
    settings = {
        'resegmentRange': [-1000, 2000], # Excludes air and bone (HU scale) for feature calculation
        'label': 2, # Label 2 is nodule
        'interpolator': 'sitkBSpline',
        'resampledPixelSpacing': [1.0, 1.0, 1.0]
    }

    try:
        extractor = SaveCropFeatureExtractor(**settings)
        features = extractor.execute(ct_path, nodule_path)

        row_data = {"PatientID": pid, "StudyInstanceUID": study_id}
        row_data.update(features)

        file_exists = os.path.exists(FEATURES_CSV)
        with open(FEATURES_CSV, "a", newline='') as f:
            w = csv.DictWriter(f, fieldnames=row_data.keys())
            if not file_exists:
                w.writeheader()
                w.writerow(row_data)
    except Exception as e:
        print(f"Failed to extract features for patient {pid}, study {study_id}: {e}")
        raise e