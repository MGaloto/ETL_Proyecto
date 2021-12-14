from transform import tablon, today

# Crear tabla si no existe
# Validar si existe el registro
# Insertar los datos en MySql


num_rows_tablon = len(tablon.index)


if num_rows_tablon != 1:
    print('Hay un conflicto en la actualizacion de los datos')
    #return

nombre_cols_sql = [  '' + col +  ''  for col in tablon.columns]
print(nombre_cols_sql)



# Query SQL

sql_create_btc_valores = """
        CREATE TABLE IF NOT EXISTS btcvalores (
                fecha DATE PRIMARY KEY,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR (50),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR (50),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR (50),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR (50),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR (50),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR(50) ,
                {} DECIMAL (20, 2)
        )
        """.format(*nombre_cols_sql)


# Conexion a Base de Datos

import pymysql

with open('Keys.txt') as claves: keys = [clave for clave in claves]

connection = pymysql.connect(
host= keys[0].strip('\n'),
user= keys[1].strip('\n'),
password= keys[2].strip('\n'),
db='sakila' )


cursor = connection.cursor()

cursor.execute(sql_create_btc_valores)

#cursor.commit()



#cursor.close()
#connection.close()