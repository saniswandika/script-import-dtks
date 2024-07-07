import pandas as pd
import mysql.connector

# Fungsi untuk memasukkan data ke database berdasarkan jumlah yang diinginkan
def masukkan_data(conn, jumlah_data, dataframe, sheet_name):
    cursor = conn.cursor()

    # Mengubah dataframe menjadi data SQL dan memasukkan ke database
    for index, row in dataframe.head(jumlah_data).iterrows():
        sql = """
            INSERT INTO db_dtks_baru (
                id,NIK, No_KK, Nama, Tempat_Lahir, Tanggal_Lahir, Ibu_Kandung, Jenis_Kelamin, Jenis_Pekerjaan, 
                Status_Kawin, Alamat, Rt, Rw, Kelurahan, Kecamatan, Agamaa, SHDK, PDDK_Akhir, Sumber_Data, 
                Status_Dtks, Program_Bansos, Kabupaten, Status_PPKS, Kebutuhan_PPKS, Rencana_Intervensi, 
                Bentuk_Rehabilitasi_Sosial, Sistem_Sumber, Program_Dinsos, Laporan_Hasil_Assesment, 
                Permasalahan_PPKS, Jenis_Pelatihan, Penetapan
            ) 
            VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        """
        values = (
            row['id'],row['NIK'], row['No_KK'], row['Nama'], row['Tempat_Lahir'], row['Tanggal_Lahir'],
            row['Nama_Ibu_Kandung'], row['Jenis_Kelamin'], row['Jenis_Pekerjaan'], row['Status_Kawin'],
            row['Alamat'], row['RT'], row['RW'], row['Kelurahan'], row['Kecamatan'], row['Agamaa'],
            row['SHDK'], row['PDDK_Akhir'], row['Sumber_Data'], row['Status_Dtks'], row['Program_Bansos'],
            row['Kabupaten'], row['Status_PPKS'], row['Kebutuhan_PPKS'], row['Rencana_Intervensi'],
            row['Bentuk_Rehabilitasi_Sosial'], row['Sistem_Sumber'], row['Program_Dinsos'],
            row['Laporan_Hasil_Assesment'], row['Permasalahan_PPKS'], row['Jenis_Pelatihan'],
            None,  # Setting 'penetapan' to None for null value
        )

        values = [None if pd.isna(val) else val for val in values]
        try:
            cursor.execute(sql, values)
            print(f"Data dari sheet '{sheet_name}' berhasil dimasukkan untuk index ke", index)
        except Exception as e:
            print(f"Gagal memasukkan data dari sheet '{sheet_name}' untuk index ke {index}. Error: {str(e)}")

    conn.commit()
    cursor.close()

# Membaca file Excel menggunakan pandas
print("Sedang membaca file Excel...")
file_excel = 'DTKS_SK_17_MEI_V1.xlsx'

# Membaca kedua sheet ('dtks' dan 'non dtks') dari file Excel
print("Sedang membaca sheet 'dtks'...")
dtks_dataframe = pd.read_excel(file_excel, sheet_name='dtks')
print("Sheet 'dtks' berhasil dibaca!")

# Membaca kedua sheet ('dtks' dan 'non dtks') dari file Excel
print("Sedang membaca sheet 'non dtks'...")
non_dtks_dataframe = pd.read_excel(file_excel, sheet_name='non dtks')
print("Sheet 'non dtks' berhasil dibaca!")

# Koneksi ke database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='dtks',
)

# Input jumlah data yang ingin dimasukkan untuk masing-masing sheet
try:
    jumlah_data_dtks = int(input("Masukkan jumlah data 'dtks' yang ingin dimasukkan: "))
    if jumlah_data_dtks > 0:
        masukkan_data(conn, jumlah_data_dtks, dtks_dataframe, 'dtks')  # Memasukkan dataframe dtks ke dalam database
    else:
        print("Jumlah data untuk 'dtks' harus lebih besar dari 0.")

    jumlah_data_non_dtks = int(input("Masukkan jumlah data 'non dtks' yang ingin dimasukkan: "))
    if jumlah_data_non_dtks > 0:
        masukkan_data(conn, jumlah_data_non_dtks, non_dtks_dataframe, 'non dtks')  # Memasukkan dataframe non dtks ke dalam database
    else:
        print("Jumlah data untuk 'non dtks' harus lebih besar dari 0.")
except ValueError:
    print("Masukkan jumlah data yang valid (angka).")

# Tutup koneksi
conn.close()
