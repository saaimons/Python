import pyodbc
import datetime
from debugger import *
import parserCsvToSql as tablas

class sandbox:
    
    def __init__(self):    
        self.tag = "sandbox"
        self.debug = debugger(self.tag)
        self.conn = pyodbc.connect(
            "Driver={SQL Server Native Client 11.0};"
            "Server=127.0.0.1,1433;"
            "UID=;"
            "PWD=;"
            "Database=;"
            "Trusted_Connection=yes"
        )

        """
        Todos los statements son ejecutados utilizando cursor.execute(query)

        cursor.execute(select AddressID, City from Person.Address order by AddressID asc)
        row = cursor.fetchval()
        print(row[0])
        print(row.City)

        """
        self.cursor = self.conn.cursor()

        """
        variables generales
        """
        self.min = 0
        self.x = 1
        self.errores = []
        self.dbtablas = []


    def checkifExists(self,mTabla):
        '''
        Verificación de existencia de tabla
        IF EXISTS
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
                while verif == False:
                    for i in rows:
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
                error = str(self.debug.getTime()) +"\nERROR: "+str(tag2)+"->"+str(tag3)+"-> Tabla: "+str(mTabla)+" inexistente."
                self.errores.append(error)
                print("                      "+str(tag2)+" "+str(tag3)+"->Tabla: '"+str(mTabla)+"' es inexistente.")
                print(":END Check")
                print("/--------------------------/")
                return False
        print()
        
        print(":END Check")
        print("/--------------------------/")
        self.debug.agregarInfo(self.errores)
        self.debug.start(True,False)
        

      #  print(" Verificado") if checkifExsits("12345")== True else print (" No Verificado")
    
  
    
    '''
    Creacion de tablas
    CREATE TABLE
    '''
    def crearTablaBeneficios(self):
        '''
        FORMATO 1
        '''
        tag2 = "Creacion de Tabla"
        mTabla = "MiTabla1"
        try:
            self.cursor.execute("""CREATE TABLE      
                            MiTabla1 
                            (
                            "id"            INT NOT NULL IDENTITY(1,1),
                            "Nombre"        VARCHAR(255)     ,
                            "Descripcion1"  VARCHAR(2048)    ,
                            "Descripcion2"  VARCHAR(2048)    
                            )          
                            """
                            )
            
            self.conn.commit()
            print("Tabla MiTabla1 Creada Correctamente")
            return True
        except:
            error = str(self.debug.getTime() +"ERROR: "+str(tag2)+"-> Tabla: "+str(mTabla)+" es posible que ya exista.")
            self.errores.append(error)
            self.debug.agregarInfo(self.errores)
            self.debug.start(False,False)
            return False
        
        
            
    def crearTabla_2(self):
        '''
        Create Table
        '''
        self.cursor.execute("""CREATE TABLE "TABLA_TEST2" (
            "id" INT NULL DEFAULT 'NULL',
            "Nombre" VARCHAR(50) NULL DEFAULT ''NULL',
            "Descripción1" VARCHAR(255) NULL DEFAULT ''NULL'' ,
            "Descripción2" VARCHAR(255) NULL DEFAULT ''NULL'' """
        )
        self.conn.commit()         
    
    def agregarDatos(self):
        '''
        Insercion de datos
        INSERT INTO
        '''
        g = tablas.valores
        c = tablas.columnas
        marker = []
        print(":Comienzo de Agregado de Datos")
        x = len(tablas.columnas)
        max = x
        
        for i in range(tablas.n):
            self.cursor.execute("""INSERT INTO MiTabla1 (Nombre) VALUES (?)""",["'"+str(g[i])+"'"])
            self.conn.commit()
            print("ID "+ str(i)+"----------------------------->Inicio de actualización")
            for a in range(max): 
                self.cursor.execute("""UPDATE MiTabla1 set """+str(tablas.columnas[a])+""" = (?) WHERE id ="""+ str(i+1),["'"+str(tablas.valores[a+self.min])+"'"])
                self.conn.commit()
                #time.sleep(1)
                print(str(a)+"  ->  "+str(tablas.columnas[a])+" ->   "+str(tablas.valores[a+self.min]))
                
            self.min = (self.min + max)
            #print("MIN " + str(self.min))
            print("ID "+ str(i)+"----------------------------->Fin de actualización\n")
            #print("Fin UPDATE")
        self.min=0
        print(":Finn de agregadod de datos")
        #print("FIN INSERT")

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
        
