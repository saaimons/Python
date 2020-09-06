import pyodbc

class sqlconn:
    
    def __init__(self):    
        self.tag = "sqlconn"
        self.min = 0
        self.x = 1
        self.errores = []
        self.dbtablas = []
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
            
                    elif i >= rows:
                        raise Exception()
            except:
                print("Ha ocurrido un ERROR")
                error = "\nERROR: "+str(tag2)+"->"+str(tag3)+"-> Tabla: "+str(mTabla)+" inexistente."
                print("                      "+str(tag2)+" "+str(tag3)+"-> '"+str(mTabla)+"' es inexistente.")
                print(":END Check")
                print("/--------------------------/")
                return False

    def agregarDatos(self,tabla):
            tablas = parser(tabla)
            
            x = tablas.getColumnas()
            g = tablas.getValores()
            n = tablas.n
            f = tablas.columnas
            marker = []
            print(":Comienzo de Agregado de Datos")
            x = len(f)
            max = x
            
            for i in range(tablas.n):
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


conexion = sqlconn()
conexion.checkifExists("tabla_1")
