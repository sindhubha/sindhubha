echo Create environment if does not exist
conda env list | find /i "fastapi-service"
if ERRORLEVEL 1 (
call conda env create -f environment.yml)
echo Build server executable folder
call conda activate fastapi-service
call pyinstaller AusDataProvian-EMS-V1.spec --noconfirm

echo Build windows_server executable folder
call pyinstaller AusServiceweg-EMS-V1.spec --noconfirm
echo Successfully built the windows service install folder
