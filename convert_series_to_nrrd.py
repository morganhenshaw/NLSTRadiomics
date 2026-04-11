import SimpleITK as sitk
import subprocess
import os

def convert_CT_to_nrrd(input_folder, output_folder):
    reader = sitk.ImageSeriesReader()
    dicom_names = reader.GetDCMSeriesFileNames(input_folder)
    reader.SetFileNames(dicom_names)
    ct_image = reader.Execute()
    sitk.WriteImage(ct_image, os.path.join(output_folder, "CT.nrrd"))

def convert_SEG_to_nrrd(input_file, output_folder):
    command = ["segimage2itkimage", "--inputDICOM", input_file, "--outputDirectory", output_folder, "--outputType", "nrrd", "--prefix", "SEG"]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Conversion of SEG to .nrrd failed: {e}") # Maybe change this not to print, but to somehow be saved in status variable?
        return False

def rename_masks(timestep_path):
    os.rename(os.path.join(timestep_path, f"SEG-1.nrrd"), os.path.join(timestep_path, "lung.nrrd"))
    os.rename(os.path.join(timestep_path, f"SEG-2.nrrd"), os.path.join(timestep_path, "nodule.nrrd"))
    os.remove(os.path.join(timestep_path, f"SEG-meta.json"))