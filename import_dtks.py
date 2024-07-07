import pandas as pd
import mysql.connector

# Fungsi untuk memasukkan data ke database berdasarkan jumlah yang diinginkan
def masukkan_data(conn, jumlah_data, dataframe):
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
            print("Data berhasil dimasukkan untuk index ke", index)
        except Exception as e:
            print(f"Gagal memasukkan data untuk index ke {index}. Error: {str(e)}")

    conn.commit()
    cursor.close()

# Membaca file Excel menggunakan pandas
print("Sedang membaca file Excel...")
file_excel = 'data_testing.xlsx'
dataframe = pd.read_excel(file_excel)
print("File Excel berhasil dibaca!")

# Koneksi ke database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='dtks',
)

# Input jumlah data yang ingin dimasukkan
try:
    jumlah_data = int(input("Masukkan jumlah data yang ingin dimasukkan: "))
    if jumlah_data > 0:
        masukkan_data(conn, jumlah_data, dataframe)  # Memasukkan dataframe ke dalam fungsi masukkan_data
    else:
        print("Jumlah data harus lebih besar dari 0.")
except ValueError:
    print("Masukkan jumlah data yang valid (angka).")

# Tutup koneksi
conn.close()
