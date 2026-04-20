import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DOWNLOAD_DIR = os.path.join(BASE_DIR, "nlst")
COHORT_CSV = os.path.join(BASE_DIR, "patient_cohort.csv")
HEADERS_CSV = os.path.join(BASE_DIR, "ct_headers.csv")
FEATURES_CSV = os.path.join(BASE_DIR, "radiomic_features.csv")
TIMING_LOG = os.path.join(BASE_DIR, "timing_log.csv")
NODULES_CSV = os.path.join(BASE_DIR, "nodules.csv")

CREDENTIALS_PATH = r"Z:\Research\RADONC_S\Krishni\MorganHenshaw\nlst-radiomics-b0de7f6a4d17.json"