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

    def insertWhereAND(self, table: str, listColumns: list, listValues: list, listParameters: list, listValuesParameters: str) -> None:
        """
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
        parameters += ";"

        query = f"INSERT INTO {table}({columns}) VALUES({values}) WHERE {parameters}"
        self.cursor.execute(query)
        self.commit()


prueba = manejadorDB(user="root", password="root", host="localhost")
prueba.useDataBase(nameDB="admin001000")
prueba.insertWhereAND(table="articulo", listColumns=["hola", "prueba"], listValues=[
                      "holaValue", "pruebaValue"], listParameters=["a"], listValuesParameters=["a"])
