import pandas as pd
import os
from idc_index import index
import pydicom

client = index.IDCClient()
excel_path = "1_Table/Image.xlsx"
base_data_path = "/nlst"
output_csv = "features.csv"

def get_features():
	metadata = pd.read_excel(excel_path)
	all_patient_rows = []
	for idx, row in metadata.iterrows():
		pid = str(row['ID'])
		sid = str(row['StudyInstanceUID'])
		patient_dir = os.path.join(base_data_path, pid, sid)
		print(f"Processing Patient {pid}")
		try:
			sr_df = client.sr_to_pandas(patient_dir)
			patient_features = sr_df.pivot_table(
				index='ID',
				columns='concept',
				values='value',
				aggfunc='first'
			).reset_index()
			all_patient_rows.append(patient_features)
		except Exception as e:
			print(f"Error processing {pid}: {e}")

	final_df = pd.concat(all_patient_rows, ignore_index=True)
	final_df.to_csv(output_csv, index=False)

features = []

def find_measurements(sequence):
	for item in sequence:
		if item.ValueType == 'NUM':
			name = item.ConceptNameCodeSequence[0].CodeMeaning
			val = item.MeasuredValueSequence[0].NumericValue
			features.append({"Feature": name, "Value": val})
		if hasattr(item, 'ContentSequence'):
			find_measurements(item.ContentSequence)

def check_features():
	sr_path = "/nlst/100012/1.2.840.113654.2.55.3832109283939010833855886500020760484/SR_1.2.276.0.7230010.3.1.3.481037312.8500.1761239665.977735.16ee396f-48b3-4405-bee7-988fb0b7e222.dcm"
	ds = pydicom.dcmread(sr_path)
	if hasattr(ds, 'ContentSequence'):
		find_measurements(ds.ContentSequence)
	df = pd.DataFrame(features)
	df.to_csv("features_from_single_sr.csv", index=False)


if __name__ == "__main__":
	check_features()
