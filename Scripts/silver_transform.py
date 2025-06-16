import os
import pandas as pd

BRONZE_FOLDER = "data/bronze/"
SILVER_FOLDER = "data/silver/"
CHUNKSIZE = 500_000

os.makedirs(SILVER_FOLDER, exist_ok=True)

def transform_to_silver():
    bronze_files = [f for f in os.listdir(BRONZE_FOLDER) if f.endswith(".parquet")]
    print(f"📄 Archivos en Bronze: {len(bronze_files)}")

    for file in bronze_files:
        try:
            print(f"\n🔄 Transformando: {file}")
            df = pd.read_parquet(os.path.join(BRONZE_FOLDER, file))

            # Convertir en datetime
            for col in ["star_time", "end_time", "started_at", "ended_at"]:
                if col in df.columns: 
                    df[col] = pd.to_datetime(df[col], errors = "coerce")
            
            if 'started_at' in df.columns and 'start_time' not in df.columns:
                df['start_time'] = df['started_at']
            if 'ended_at' in df.columns and 'end_time' not in df.columns:
                df['end_time'] = df['ended_at']

            # Calcular trip_duration si no existe
            if 'trip_duration' not in df.columns or df['trip_duration'].isnull().all():
                df['trip_duration'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60

            # Quitar valores absurdos o negativos
            df = df[df['trip_duration'] > 0]

            # Extraer campos de tiempo
            df['start_hour'] = df['start_time'].dt.hour
            df['start_day'] = df['start_time'].dt.day
            df['start_month'] = df['start_time'].dt.month
            df['year_month'] = df['start_time'].dt.to_period("M").astype(str)
            df['date'] = df['start_time'].dt.date

            # Estandarizar tipos de miembro
            if 'member_type' in df.columns:
                df['member_type'] = df['member_type'].str.lower().replace({
                    'subscriber': 'member',
                    'customer': 'casual'
                })

            # Guardar archivo transformado
            out_path = os.path.join(SILVER_FOLDER, file)
            df.to_parquet(out_path, index=False)
            print(f"✅ Guardado en Silver: {file}")

        except Exception as e:
            print(f"❌ Error con {file}: {e}")

    print("\n🏁 Transformación Silver completada.")

if __name__ == "__main__":
    transform_to_silver()
