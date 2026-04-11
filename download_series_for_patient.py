from idc_index import IDCClient
import os
import pydicom
import csv

client = IDCClient()
download_directory = r"Z:\Research\RADONC_S\Krishni\MorganHenshaw"
ct_headers_csv = r"Z:\Research\RADONC_S\Krishni\MorganHenshaw\ct_headers.csv"

def download_series(seriesInstanceUID):
    client.download_dicom_series(seriesInstanceUID, download_directory)

# extracts relevant CT header info for all CT series of one patient
def extract_ct_header_info(patientID):
    headers = ["Manufacturer","ManufacturerModelName","SliceThickness","KVP","DataCollectionDiameter","FilterType","FocalSpots","ConvolutionKernel","ExposureTime","XRayTubeCurrent","Exposure", "PixelSpacing"]
    headerinfo = {}
    patient_path = os.path.join(download_directory, "nlst", patientID)
    timesteps = os.listdir(patient_path)
    for timestep in timesteps:
        timestep_path = os.path.join(patient_path, timestep)
        if not os.path.isdir(timestep_path):
            continue

        ct_info = {}
        ct_series_dir = None
        for item in os.listdir(timestep_path):
            full_item_path = os.path.join(timestep_path, item)
            if os.path.isdir(full_item_path) and item.startswith('CT_'):
                ct_series_dir = full_item_path
                break # Found the CT series directory

        if ct_series_dir:
            # Now, look for a DICOM file inside the CT series directory
            dicom_files = [f for f in os.listdir(ct_series_dir) if f.endswith('.dcm')]
            if dicom_files:
                ct_file_path = os.path.join(ct_series_dir, dicom_files[0]) # Take the first one
                try:
                    ct_img = pydicom.dcmread(ct_file_path, stop_before_pixels=True)
                    for header in headers:
                        key = header
                        value = ct_img.get(header, None)
                        if value is not None:
                            ct_info[key] = value
                        else:
                            ct_info[key] = "N/A"
                    headerinfo[patientID+"_"+timestep] = ct_info
                except Exception as e:
                    print(f"Failed to read DICOM file {ct_file_path}: {e}")
            else:
                print(f"No DICOM files found in {ct_series_dir}")
        else:
            print(f"CT series directory not found in {timestep_path}")

    headers.insert(0,"Timestep")
    file_exists = os.path.exists(ct_headers_csv)
    with open(ct_headers_csv, "a", newline='') as f:
        w = csv.DictWriter(f, headers)
        if not file_exists:
            w.writeheader()
        for key,val in sorted(headerinfo.items()):
            row = {'Timestep': key}
            row.update(val)
            w.writerow(row)