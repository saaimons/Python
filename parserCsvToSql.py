import pandas as pd

class parserCSV:
    
    def __init__(self,tabla):
        self.path = './tablas/'
        self.archivo = str(self.path+tabla+'/'+tabla+'.csv')
        self.activarDebug = True
        self.columnas = []   
        self.external_config = pd.read_csv(self.archivo)
        self.n = len(self.external_config)
        self.ids = []
        if self.n == 0:
            print ("config file is Empty!")
            print ("Exiting in 5 sec...")
            exit()
        self.qtyItems = len(self.ids)
        self.qtyColumnas = len(self.columnas)

    def debug(self,activar):
        if (activar == True):
            print("CSV TO SQL\n")
            print("Archivo --> "+ str(self.archivo))
            print("Cantidad de Columnas -->"+str(self.qtyColumnas))
            print("Cantidad de Filas --> "+ str(self.qtyItems))
            print("Contenido...")
            print("\n"+str(self.n)+"\n")
            print(self.qtyItems)
            for z in range(self.qtyItems):
                    print(self.getColumnas())
                    print(self.getValores())
            print("Debug Finalizado")
            print("\n")  
            
    def getColumnas(self):
        for x in self.external_config:
            self.columnas.append(x)
        print("Columnas obtenidas!")
        return str(self.columnas).replace("\'","").replace("[","(").replace("]",")")
            
    def getValores(self):
        valores = []
        for x in range(self.n):
            for a in self.columnas:
                valores.append(self.external_config[a][x])
        return valores   
    