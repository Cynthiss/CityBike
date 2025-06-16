import os
import pandas as pd

# Rutas
SILVER_FOLDER = "data/silver/"
GOLD_FOLDER = "data/gold/"
os.makedirs(GOLD_FOLDER, exist_ok=True)

# Cargar todos los archivos Silver
def load_all_silver():
    files = [f for f in os.listdir(SILVER_FOLDER) if f.endswith(".parquet")]
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(os.path.join(SILVER_FOLDER, f))
            dfs.append(df)
        except Exception as e:
            print(f"❌ Error leyendo {f}: {e}")
    return pd.concat(dfs, ignore_index=True)

# Agregaciones Gold
def generate_gold(df):
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df.dropna(subset=['start_time'], inplace=True)
    df['date'] = df['start_time'].dt.date
    df['year_month'] = df['start_time'].dt.to_period('M').astype(str)
    df['year'] = df['start_time'].dt.year

    # 1. Viajes por día
    df.groupby('date').size().reset_index(name='num_rides') \
      .to_parquet(os.path.join(GOLD_FOLDER, "rides_by_day.parquet"), index=False)

    # 2. Viajes por estación y mes
    df.groupby(['year_month', 'start_station_name']).size().reset_index(name='num_rides') \
      .to_parquet(os.path.join(GOLD_FOLDER, "rides_by_station_month.parquet"), index=False)

    # 3. Viajes por hora del día
    df.groupby('start_hour').size().reset_index(name='rides') \
      .to_parquet(os.path.join(GOLD_FOLDER, "rides_by_hour.parquet"), index=False)

    # 4. Duración promedio por tipo de usuario
    df.groupby('member_type')['trip_duration'].mean().reset_index() \
      .to_parquet(os.path.join(GOLD_FOLDER, "avg_duration_by_member_type.parquet"), index=False)

    # 5. Distribución por tipo de usuario
    df['member_type'].value_counts().reset_index().rename(columns={'index': 'member_type', 'member_type': 'ride_count'}) \
      .to_parquet(os.path.join(GOLD_FOLDER, "member_distribution.parquet"), index=False)

    # 6. Rutas más utilizadas
    df.groupby(['start_station_name', 'end_station_name']).size().reset_index(name='num_rides') \
      .sort_values(by='num_rides', ascending=False) \
      .to_parquet(os.path.join(GOLD_FOLDER, "top_routes.parquet"), index=False)

    # 7. Viajes por año
    df.groupby('year').size().reset_index(name='num_rides') \
      .to_parquet(os.path.join(GOLD_FOLDER, "rides_by_year.parquet"), index=False)

    # 8. Viajes por mes
    df.groupby('year_month').size().reset_index(name='num_rides') \
      .to_parquet(os.path.join(GOLD_FOLDER, "rides_by_month.parquet"), index=False)

    # 9. Ruta más popular por tipo de usuario
    df.groupby(['member_type', 'start_station_name', 'end_station_name']) \
      .size().reset_index(name='num_rides') \
      .to_parquet(os.path.join(GOLD_FOLDER, "top_routes_by_type.parquet"), index=False)

    print("✅ Archivos Gold generados correctamente.")

# Ejecutar script
if __name__ == "__main__":
    print("🚴‍♂️ Cargando datos Silver...")
    df = load_all_silver()
    print(f"🔢 Total de registros cargados: {len(df)}")
    generate_gold(df)
