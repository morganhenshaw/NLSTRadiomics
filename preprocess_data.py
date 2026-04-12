from convert_series_to_nrrd import convert_CT_to_nrrd, convert_SEG_to_nrrd, delete_series
from extract_ct_header import extract_ct_header_info
from config import BASE_DIR, DOWNLOAD_DIR, COHORT_CSV
from extract_features import extract_features

import pandas as pd
from idc_index import IDCClient

client = IDCClient()

# batch_size is number of series to be processed
def main(batch_size=2):
    df = pd.read_csv(COHORT_CSV)
    work_queue = df[df['Status'] == 'Unprocessed'].head(batch_size)
    total_in_batch = len(work_queue)
    if total_in_batch == 0:
        print("No unprocessed data found.")
        return

    # Use enumerate to get the current count (i) starting at 1
    for i, (index, row) in enumerate(work_queue.iterrows(), 1):
        pid = row['PatientID']
        study_uid = row['StudyInstanceUID']
        ct_uid = row['CT_SeriesInstanceUID']
        seg_uid = row['SEG_SeriesInstanceUID']
        print(f"\n[{i}/{total_in_batch}] Processing Patient: {pid} , Study: {study_uid}")
        print("-" * 30)
        
        try:
            print(f"Downloading CT and SEG series")
            client.download_from_selection(seriesInstanceUID=ct_uid, downloadDir=BASE_DIR)
            client.download_dicom_series(ct_uid, DOWNLOAD_DIR)
            client.download_dicom_series(seg_uid, DOWNLOAD_DIR)
            
            print(f"Extracting CT header information")
            extract_ct_header_info(pid, study_uid, ct_uid)
            print(f"Converting CT and SEG to .nrrd format")
            convert_CT_to_nrrd(pid, ct_uid, study_uid)
            convert_SEG_to_nrrd(pid, seg_uid, study_uid)
            print(f"Extracting radiomic features")
            extract_features(pid, study_uid)
            print(f"Deleting series data")
            delete_series(pid, study_uid, ct_uid, seg_uid)

            df.at[index, 'Status'] = 'Completed'
            
        except Exception as e:
            print(f"Failed to process {pid}, Study: {study_uid}: {e}")
            df.at[index, 'Status'] = f"Failed: {str(e)[:50]}"
        
        df.to_csv(COHORT_CSV, index=False)

    print(f"\nProcessed {batch_size} series.")