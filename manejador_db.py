import mysql.connector as connector


class manejadorDB:
    """
    Objeto que usa mysql.connector para la manipulacion de bases de datos mysql a traves de los metodos establecidos
    """

    def __init__(self, user: str, password: str, host: str) -> None:
        """
        Establece la conexion con la base de datos a traves de las credenciales ingresadas por la persona
        """
        self.conexion = connector.connect(
            user=user, password=password, host=host)
        self.cursor = self.conexion.cursor()

    def useDataBase(self, nameDB: str) -> None:
        """
        Recibe como parametro el nombre de la base de datos que se quiere usar
        """
        self.cursor.execute(f"USE {nameDB};")

    def commit(self) -> None:
        """
        Hace commit de los cambios realizados en la base de datos
        """
        self.cursor.execute(f"COMMIT;")

    def insertColumnsWhereAND(self, table: str, listColumns: list, listValues: list, listParameters: list, listValuesParameters: list) -> None:
        """
        INSERT INTO table(listColumns) VALUES(listValues) WHERE listParameters = listValuesParameters AND... ;\n
        Atraves del parametro listColumns se le pasa el nombre de las columnas de la base de datos donde se van a insertar los valores,
        la listValues corresponde a los valores que se van a insertar en listColumns, la listParameters son la lista de los parametros
        a usar para la insercion de datos y la listValuesParameters son la lista de valores correspondientes a la lista de parametros.
        Para unir los parametros este metodo usa la sentencia logica AND
        """
        columns = ""
        values = ""
        parameters = ""

        for columnas in listColumns:
            columns += f"{columnas}, "
        columns = columns[0:len(columns)-2]

        for valores in listValues:
            values += f"{valores}, "
        values = values[0:len(values)-2]

        for parametros, valoresParametros in zip(listParameters, listValuesParameters):
            parameters += f"{parametros}=\'{valoresParametros}\' AND "
        parameters = parameters[0:len(parameters)-5]

        query = f"INSERT INTO {table}({columns}) VALUES({values}) WHERE {parameters};"
        self.cursor.execute(query)
        self.commit()

    def insertColumnsWhereOR(self, table: str, listColumns: list, listValues: list, listParameters: list, listValuesParameters: list) -> None:
        """
        INSERT INTO table(listColumns) VALUES(listValues) WHERE listParameters = listValuesParameters OR... ;\n
        Atraves del parametro listColumns se le pasa el nombre de las columnas de la base de datos donde se van a insertar los valores,
        la listValues corresponde a los valores que se van a insertar en listColumns, la listParameters son la lista de los parametros
        a usar para la insercion de datos y la listValuesParameters son la lista de valores correspondientes a la lista de parametros.
        Para unir los parametros este metodo usa la sentencia logica OR
        """
        columns = ""
        values = ""
        parameters = ""

        for columnas, valores in zip(listColumns, listValues):
            columns += f"{columnas}, "
            values += f"{valores}, "
        columns = columns[0:len(columns)-2]
        values = values[0:len(values)-2]

        for parametros, valoresParametros in zip(listParameters, listValuesParameters):
            parameters += f"{parametros}=\'{valoresParametros}\' OR "
        parameters = parameters[0:len(parameters)-4]
        parameters += ";"

        query = f"INSERT INTO {table}({columns}) VALUES({values}) WHERE {parameters};"
        self.cursor.execute(query)
        self.commit()

    def insertRow(self, table: str, listColumns: list, listValues: list) -> None:
        """
        INSERT INTO table(listColumns) VALUES(listValues);\n
        Se debe pasar a traves del parametro listColumns una lista de strings que contengan el nombre de las columnas
        donde se va a insertar la informacion pasada a traves del parametro listValues
        """
        columns = ""
        values = ""

        for columnas, valores in zip(listColumns, listValues):
            columns += f"{columnas}, "
            values += f"{valores}, "

        columns = columns[0:len(columns)-2]
        values = values[0:len(values)-2]

        query = f"INSERT INTO {table}({columns}) VALUES({values});"
        self.cursor.execute(query)
        self.commit()
