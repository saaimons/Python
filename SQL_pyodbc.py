import pyodbc
import sys
from parserCSV import *

class sqlconn:
    
    def __init__(self):    
        self.tag = "sqlconn"
        self.min = 0
        self.x = 1
        self.errores = []
        self.dbtablas = []
        self.conn = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};"
            "Server=127.0.0.1,56565;"
            "UID=sa;"
            "PWD=servidordeprueba;"
            "Database=android_appdata;"
            "Trusted_Connection=no"
        )

        """
        Todos los statements son ejecutados utilizando cursor.execute(query)

        cursor.execute(select AddressID, City from Person.Address order by AddressID asc)
        row = cursor.fetchval()
        print(row[0])
        print(row.City)

        """
        self.cursor = self.conn.cursor()

    def checkifExists(self,mTabla):
        '''
        Verificación de existencia de tabla
        '''
        tag2 = "Check"
        print("/--------------------------/")
        print(":Init Check")
        
        self.cursor.execute(
            """
            SELECT * from INFORMATION_SCHEMA.TABLES   
            """
        )
        row = self.cursor.fetchall()
        if row:
            a = len(row)
            print(" Las tablas existentes son: "+str(a))
            for i in row: 
                self.dbtablas.append(i)
            rows = range(len(self.dbtablas))
            for i in rows:
                print("                 "+str(i)+"  ->  "+self.dbtablas[i][2])
            
            print()
            verif = False
            tag3= "Tablas"
            try:
                for i in rows:
                    prev = i
                    if self.dbtablas[i][2] == mTabla:
                        print("                 "+str(i)+"  ->  "+self.dbtablas[i][2])
                        print("     Tabla: "+str(mTabla)+" existe")
                        #self.dbtablas.clear()
                        print(":END Check")
                        print("/--------------------------/")
                        return True
            
                    elif i == rows:
                        raise Exception()
            except:
                print("Ha ocurrido un ERROR")
                error = "\nERROR: "+str(tag2)+"->"+str(tag3)+"-> Tabla: "+str(mTabla)+" inexistente."
                for i in range(len(sys.exc_info())):
                    print(str(sys.exc_info()[i]))
                print("                      "+str(tag2)+" "+str(tag3)+"-> '"+str(mTabla))
                print(":END Check")
                print("/--------------------------/")
                return False

    def crearTabla(self,tabla):
            '''
            FORMATO 1
            '''
            tag2 = "Creacion de Tabla"
            
            try:
                self.cursor.execute("""CREATE TABLE      
                                """+tabla+""" 
                                (
                                "id"            INT NOT NULL IDENTITY(1,1),
                                "Nombre"        VARCHAR(128)      NULL    ,
                                "Descripcion"   VARCHAR(128)      NULL    ,
                                )          
                                """
                                )
                
                self.conn.commit()
                print("Tabla "+tabla+" Creada Correctamente")
                return True
            except:
                error = str("ERROR: "+str(tag2)+"-> Tabla: "+str(tabla)+" es posible que ya exista.")
                self.errores.append(error)
                return False
    
    def agregarDatos(self,tabla):
            mTabla = parserCSV(tabla)   
            x = mTabla.getColumnas()
            g = mTabla.getValores()
            n = mTabla.n
            f = mTabla.columnas
            marker = []
            print(":Comienzo de Agregado de Datos")
            x = len(f)
            max = x
            
            for i in range(mTabla.n):
                self.cursor.execute("""INSERT INTO """+tabla+""" (Nombre) VALUES (?)""",["'"+str(g[i])+"'"])
                self.conn.commit()
                print("ID "+ str(i)+"----------------------------->Inicio de actualización")
                for a in range(max): 
                    self.cursor.execute("""UPDATE """+tabla+""" set """+str(f[a])+""" = (?) WHERE id ="""+ str(i+1),["'"+str(g[a+self.min])+"'"])
                    self.conn.commit()
                    #time.sleep(1)
                    print(str(a)+"  ->  "+str(f[a])+" ->   "+str(g[a+self.min]))
                    
                self.min = (self.min + max)
                print("ID "+ str(i)+"----------------------------->Fin de actualización\n")
            self.min=0
            print(":Finn de agregado de datos")

    def borrarTabla(self,tabla):
        self.cursor.execute("""drop table """+tabla)
        self.conn.commit()
        print("     Tabla: "+str(tabla)+" ha sido eliminada X.X")

    def clearCache(self):
        self.dbtablas.clear()
        self.errores.clear()
    
    def cerrarConn(self):
        self.conn.close()
        print(":CONEXIÓN CERRADA:")


tablaNombre="tabla_1"
conexion = sqlconn()
conexion.checkifExists(tablaNombre)
conexion.clearCache()
conexion.crearTabla(tablaNombre)
conexion.clearCache()
conexion.agregarDatos(tablaNombre)
conexion.clearCache()
conexion.cerrarConn()

