from idc_index import IDCClient
import os
import pydicom
import csv
from config import DOWNLOAD_DIR, HEADERS_CSV

def extract_ct_header_info(pid, study_uid, ct_uid):
    base_headers = [
        "Manufacturer", "ManufacturerModelName", "SliceThickness", "KVP", 
        "DataCollectionDiameter", "FilterType", "FocalSpots", "ConvolutionKernel", 
        "ExposureTime", "XRayTubeCurrent", "Exposure", "PixelSpacing"
    ]
    try:
        ct_series_dir = os.path.join(DOWNLOAD_DIR, pid, study_uid, "CT_" + ct_uid)
        dicom_files = [f for f in os.listdir(ct_series_dir) if f.endswith('.dcm')]
        ct_file_path = os.path.join(ct_series_dir, dicom_files[0])
        ct_img = pydicom.dcmread(ct_file_path, stop_before_pixels=True)
        ct_info = {} 
        for header in base_headers:
            ct_info[header] = ct_img.get(header, "N/A")         
    except Exception as e:
        print(f"Failed to read CT header information for patient {pid}: {study_uid}: {e}")
        raise e

    csv_headers = ["PatientID", "StudyInstanceUID"] + base_headers
    file_exists = os.path.exists(HEADERS_CSV)
    with open(HEADERS_CSV, "a", newline='') as f:
        w = csv.DictWriter(f, fieldnames=csv_headers)
        if not file_exists:
            w.writeheader()
        row = {
            "PatientID": pid,
            "StudyInstanceUID": study_uid
        }
        row.update(ct_info)
        w.writerow(row)