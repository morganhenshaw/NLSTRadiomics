import os
from google.cloud import bigquery

# Environment setup
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"Z:\Research\RADONC_S\Krishni\MorganHenshaw\nlst-radiomics-b0de7f6a4d17.json"
client = bigquery.Client(project="nlst-radiomics")

lung_nodule_seg_series_descriptions = [
    'AIMI lung and nodule radiologist 5 corrected segmentation',
    'AIMI lung and nodule radiologist 4 corrected segmentation',
    'AIMI lung and nodule radiologist 8 corrected segmentation',
    'AIMI lung and nodule AI segmentation'
]

query = f"""
SELECT
    DISTINCT seg.SeriesInstanceUID as SEG_SeriesInstanceUID,
    seg.SeriesDescription AS SEG_SeriesDescription,
    ct.SeriesInstanceUID as CT_SeriesInstanceUID,
    ct.PatientID
FROM
    `bigquery-public-data.idc_v23.dicom_all` AS ct
JOIN
    `bigquery-public-data.idc_v23.dicom_all` AS seg
ON
    ct.PatientID = seg.PatientID
WHERE
    ct.collection_id = 'nlst'
    AND seg.collection_id = 'nlst'
    AND ct.Modality = 'CT'
    AND seg.Modality = 'SEG'
    AND seg.SeriesDescription IN ({', '.join([f"'{s}'" for s in lung_nodule_seg_series_descriptions])})
    AND EXISTS (
        SELECT 1
        FROM UNNEST(seg.ReferencedSeriesSequence) as ref_seq
        WHERE ct.SeriesInstanceUID = ref_seq.SeriesInstanceUID
    )
"""

query_result = client.query(query)
patient_cohort_unfiltered = query_result.result().to_dataframe()

# Currently there are more SEG series than CT series; this is because some CT series have AI segmentation and radiologist corrected segmentation
# "Keep" the radiologist corrected segmentation by creating a priority column: 0 if radiologist corrected, 1 if AI
ignore_seg_series_description = "AIMI lung and nodule AI segmentation"
patient_cohort_unfiltered['Priority'] = patient_cohort_unfiltered['SEG_SeriesDescription'].apply(lambda x: 1 if x == ignore_seg_series_description else 0)
partient_cohort_sorted = patient_cohort_unfiltered.sort_values(by=['CT_SeriesInstanceUID', 'Priority'])
patient_cohort = partient_cohort_sorted.drop_duplicates(subset='CT_SeriesInstanceUID', keep='first')

# create dictionary where key is PatientID and value is list of CTSeriesInstanceUIDs and SEGSeriesInstanceUIDs
patients_series_uids = {}
for patient_id in patient_cohort['PatientID'].unique():
    patient_df = patient_cohort[patient_cohort['PatientID'] == patient_id]
    ct_uids = patient_df['CT_SeriesInstanceUID'].tolist()
    seg_uids = patient_df['SEG_SeriesInstanceUID'].tolist()
    all_series_uids = ct_uids + seg_uids
    patients_series_uids[patient_id] = all_series_uids


# SANITY CHECKS 
# for i, (key, value) in enumerate(patients_series_uids.items()):
#     if i==5:
#         break
#     print(f"{key}: {value}")
# print(len(patients_series_uids))