import pandas as pd

# Leer el archivo CSV original
df = pd.read_csv('../data/driver_standings_with_info.csv')

# Configurar el número objetivo de registros
target_rows = 2500

# Calcular la distribución de registros por driver
driver_counts = df['driverId'].value_counts()
total_rows = len(df)

# Lista para almacenar los datos muestreados
sampled_dfs = []

# Muestreo proporcional por cada driver
for driver_id, count in driver_counts.items():
    # Calcular la proporción del driver respecto al total
    proportion = count / total_rows
    # Calcular cuántos registros debe tener este driver en el dataset comprimido
    target_sample = int(proportion * target_rows)
    
    if target_sample > 0:
        driver_data = df[df['driverId'] == driver_id]
        
        # Si el driver tiene menos registros que los necesarios, tomar todos
        if len(driver_data) <= target_sample:
            sampled_data = driver_data
        else:
            # Muestreo aleatorio de los registros del driver
            sampled_data = driver_data.sample(n=target_sample, random_state=42)
        
        sampled_dfs.append(sampled_data)

# Combinar todos los datos muestreados
df_compressed = pd.concat(sampled_dfs, ignore_index=True)

# Si faltan registros para llegar a 2500, agregar algunos adicionales
remaining = target_rows - len(df_compressed)
if remaining > 0:
    available_drivers = driver_counts[driver_counts > 1]
    for driver_id in available_drivers.index[:remaining]:
        driver_data = df[df['driverId'] == driver_id]
        extra = driver_data.sample(n=1, random_state=42)
        df_compressed = pd.concat([df_compressed, extra], ignore_index=True)

# Mezclar los registros para evitar agrupaciones
df_compressed = df_compressed.sample(frac=1, random_state=42).reset_index(drop=True)

# Guardar el archivo comprimido
df_compressed.to_csv('compressed_2500.csv', index=False)

print(f"✓ Archivo comprimido creado exitosamente")
print(f"✓ Registros totales: {len(df_compressed)}")
print(f"✓ Drivers únicos: {df_compressed['driverId'].nunique()}")
