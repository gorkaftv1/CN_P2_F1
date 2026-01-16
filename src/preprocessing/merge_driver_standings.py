import pandas as pd
from pathlib import Path

# Obtener la ruta del directorio del script
script_dir = Path(__file__).parent
project_dir = script_dir.parent
dataset_dir = project_dir / 'dataset'

# Leer los archivos CSV
driver_standings = pd.read_csv(dataset_dir / 'driver_standings.csv')
drivers = pd.read_csv(dataset_dir / 'drivers.csv')

# Seleccionar solo las columnas necesarias de drivers
drivers_subset = drivers[['driverId', 'forename', 'surname', 'dob', 'nationality']]

# Hacer el merge (join) por driverId
merged_df = pd.merge(
    driver_standings,
    drivers_subset,
    on='driverId',
    how='left'
)

# Guardar el resultado en un nuevo CSV
merged_df.to_csv(dataset_dir / 'driver_standings_with_info.csv', index=False)

print(f"Archivo creado exitosamente: driver_standings_with_info.csv")
print(f"Total de filas: {len(merged_df)}")
print(f"\nPrimeras filas del resultado:")
print(merged_df.head())
