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

# extracts relevant CT header info for one CT series
# def extract_ct_header_info(patientID, studyInstanceUID, CT_SeriesInstanceUID):
#     headers = ["Manufacturer","ManufacturerModelName","SliceThickness","KVP","DataCollectionDiameter","FilterType","FocalSpots","ConvolutionKernel","ExposureTime","XRayTubeCurrent","Exposure", "PixelSpacing"]
#     headerinfo = {}
#     ct_info = {}
#     ct_series_dir = os.path.join(download_directory, "nlst", patientID, studyInstanceUID, "CT_" + CT_SeriesInstanceUID)
#     dicom_files = [f for f in os.listdir(ct_series_dir) if f.endswith('.dcm')]
#     if dicom_files:
#         ct_file_path = os.path.join(ct_series_dir, dicom_files[0]) # Take the first one
#         try:
#             ct_img = pydicom.dcmread(ct_file_path, stop_before_pixels=True)
#             for header in headers:
#                 key = header
#                 value = ct_img.get(header, None)
#                 if value is not None:
#                     ct_info[key] = value
#                 else:
#                     ct_info[key] = "N/A"
#             headerinfo[patientID+"_"+studyInstanceUID] = ct_info
#         except Exception as e:
#             print(f"Failed to read DICOM file {ct_file_path}: {e}")
#     else:
#         print(f"No DICOM files found in {ct_series_dir}")

#     headers.insert(0,"Timestep")
#     file_exists = os.path.exists(ct_headers_csv)
#     with open(ct_headers_csv, "a", newline='') as f:
#         w = csv.DictWriter(f, headers)
#         if not file_exists:
#             w.writeheader()
#         for key,val in sorted(headerinfo.items()):
#             row = {'Timestep': key}
#             row.update(val)
#             w.writerow(row)