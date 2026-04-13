from convert_series_to_nrrd import convert_CT_to_nrrd, convert_SEG_to_nrrd, delete_series
from extract_ct_header import extract_ct_header_info
from config import BASE_DIR, DOWNLOAD_DIR, COHORT_CSV, TIMING_LOG
from extract_features import extract_features
import time
import pandas as pd
from idc_index import IDCClient
import os
import csv

client = IDCClient()

def log_performance(pid, study_uid, t_download, t_header, t_convert, t_features, t_total):
    file_exists = os.path.exists(TIMING_LOG)
    with open(TIMING_LOG, "a", newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["PatientID", "StudyInstanceUID", "Download_Sec", "Header_Sec", "Convert_Sec", "Features_Sec", "Total_Sec"])
        writer.writerow([pid, study_uid, round(t_download, 2), round(t_header, 2), round(t_convert, 2), round(t_features, 2), round(t_total, 2)])

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
        start_total = time.time()
        
        try:
            print(f"Downloading CT and SEG series")
            start_time = time.time()
            client.download_from_selection(seriesInstanceUID=ct_uid, downloadDir=BASE_DIR)
            client.download_dicom_series(ct_uid, DOWNLOAD_DIR)
            client.download_dicom_series(seg_uid, DOWNLOAD_DIR)
            t_download = time.time() - start_time
            
            print(f"Extracting CT header information")
            start_time = time.time()
            extract_ct_header_info(pid, study_uid, ct_uid)
            t_header = time.time() - start_time

            print(f"Converting CT and SEG to .nrrd format")
            start_time = time.time()
            convert_CT_to_nrrd(pid, ct_uid, study_uid)
            convert_SEG_to_nrrd(pid, seg_uid, study_uid)
            t_convert = time.time() - start_time

            print(f"Extracting radiomic features")
            start_time = time.time()
            extract_features(pid, study_uid)
            t_features = time.time() - start_time

            print(f"Deleting series data")
            delete_series(pid, study_uid, ct_uid, seg_uid)
            t_total = time.time() - start_total

            df.at[index, 'Status'] = 'Completed'
            print(f"DONE. Times: Download: {t_download:.1f}s | "
                  f"Header: {t_header:.1f}s | "
                  f"Convert: {t_convert:.1f}s | "
                  f"Features: {t_features:.1f}s | "
                  f"Total: {t_total:.1f}s")
            log_performance(pid, study_uid, t_download, t_header, t_convert, t_features, t_total)
            
        except Exception as e:
            print(f"Failed to process {pid}, Study: {study_uid}: {e}")
            df.at[index, 'Status'] = f"Failed: {str(e)[:50]}"
        
        df.to_csv(COHORT_CSV, index=False)

    print(f"\nProcessed {batch_size} series.")

if __name__ == "__main__":
    main(batch_size=2)