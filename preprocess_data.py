from convert_series_to_nrrd import convert_CT_to_nrrd, convert_SEG_to_nrrd, delete_series, separate_nodules
from extract_ct_header import extract_ct_header_info
from config import BASE_DIR, COHORT_CSV, TIMING_LOG, NODULES_CSV
import time
import pandas as pd
from idc_index import IDCClient
import os
import csv

client = IDCClient()

def log_performance(pid, study_uid, t_download, t_header, t_convert):
    file_exists = os.path.exists(TIMING_LOG)
    with open(TIMING_LOG, "a", newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["PatientID", "StudyInstanceUID", "Download_Sec", "Header_Sec", "Convert_Sec", "Features_Sec"])
        writer.writerow([pid, study_uid, round(t_download, 2), round(t_header, 2), round(t_convert, 2), 0])

def log_nodule_sizes(pid, study_uid, nodules_sizes):
    file_exists = os.path.exists(NODULES_CSV)
    with open(NODULES_CSV, "a", newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["PatientID", "StudyInstanceUID", "NoduleID", "Dim_X", "Dim_Y", "Dim_Z"])
        for i, nodule_size in enumerate(nodules_sizes, start=1):
            writer.writerow([pid, study_uid, i, nodule_size[0], nodule_size[1], nodule_size[2]])

# batch_size is number of series to be processed
def main(batch_size=2):
    df = pd.read_csv(COHORT_CSV)
    work_queue = df[df['Status'] == 'Unprocessed'].head(batch_size)
    total_in_batch = len(work_queue)
    if total_in_batch == 0:
        print("No unprocessed data found.")
        return

    for i, (index, row) in enumerate(work_queue.iterrows(), 1):
        pid = str(row['PatientID'])
        study_uid = str(row['StudyInstanceUID'])
        ct_uid = str(row['CT_SeriesInstanceUID'])
        seg_uid = str(row['SEG_SeriesInstanceUID'])
        print(f"\n[{i}/{total_in_batch}] Processing Patient: {pid} , Study: {study_uid}")
        print("-" * 30)
        start_total = time.time()
        
        try:
            print(f"Downloading CT and SEG series")
            start_time = time.time()
            client.download_from_selection(seriesInstanceUID=ct_uid, downloadDir=BASE_DIR)
            client.download_from_selection(seriesInstanceUID=seg_uid, downloadDir=BASE_DIR)
            t_download = time.time() - start_time
            
            print(f"Extracting CT header information")
            start_time = time.time()
            extract_ct_header_info(pid, study_uid, ct_uid)
            t_header = time.time() - start_time

            print(f"Converting CT and SEG to .nrrd format")
            start_time = time.time()
            convert_CT_to_nrrd(pid, study_uid, ct_uid)
            convert_SEG_to_nrrd(pid, study_uid, seg_uid)
            nodules_sizes = separate_nodules(pid, study_uid)
            log_nodule_sizes(pid, study_uid, nodules_sizes)
            df.at[index, 'NumNodules'] = len(nodules_sizes)
            t_convert = time.time() - start_time

            print(f"Deleting series data")
            delete_series(pid, study_uid, ct_uid, seg_uid)
            t_total = time.time() - start_total

            df.at[index, 'Status'] = 'Downloaded'
            print(f"DONE. Times: Download: {t_download:.1f}s | "
                  f"Header: {t_header:.1f}s | "
                  f"Convert: {t_convert:.1f}s | "
                  f"Total: {t_total:.1f}s")
            log_performance(pid, study_uid, t_download, t_header, t_convert)
            
        except Exception as e:
            print(f"Failed to process {pid}, Study: {study_uid}: {e}")
            df.at[index, 'Status'] = f"Failed: {str(e)[:50]}"
        
        df.to_csv(COHORT_CSV, index=False)

    print(f"\nProcessed {batch_size} series.")

if __name__ == "__main__":
    main(batch_size=2)