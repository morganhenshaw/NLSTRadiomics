import os
from google.cloud import bigquery
from config import COHORT_CSV, CREDENTIALS_PATH

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
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
    ct.StudyInstanceUID as StudyInstanceUID,
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

df = client.query(query).to_dataframe()

# Currently there are more SEG series than CT series; this is because some CT series have AI segmentation and radiologist corrected segmentation
# "Keep" the radiologist corrected segmentation by creating a priority column
ignore_desc = "AIMI lung and nodule AI segmentation"
df['Priority'] = (df['SEG_SeriesDescription'] == ignore_desc).astype(int)
df_sorted = df.sort_values(by=['CT_SeriesInstanceUID', 'Priority'])
df_clean = df_sorted.drop_duplicates(subset='CT_SeriesInstanceUID', keep='first')

final_df = df_clean[['PatientID', 'StudyInstanceUID', 'CT_SeriesInstanceUID', 'SEG_SeriesInstanceUID']]
final_df = final_df.sort_values(by=['PatientID', 'StudyInstanceUID'])
final_df.to_csv(COHORT_CSV, index=False)
final_df['NumNodules'] = 0
final_df['Status'] = "Unprocessed"
final_df.to_csv(COHORT_CSV, index=False)
print(f"PatientIDs and SeriesInstanceUIDs saved to {COHORT_CSV}")
