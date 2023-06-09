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
        USE nameDB;\n
        Recibe como parametro el nombre de la base de datos que se quiere usar
        """
        self.cursor.execute(f"USE {nameDB};")

    def createDataBase(self, nameDB: str) -> None:
        """
        CREATE DATABASE nameDB;\n
        Crea una nueva base de datos
        """
        self.cursor.execute(f"CREATE DATABASE {nameDB};")
        self.commit()

    def deleteDataBase(self, nameDB: str) -> None:
        """
        DROP DATABASE nameDB;\n
        Borra una base de datos completamente
        """
        self.cursor.execute(f"DROP DATABASE {nameDB};")
        self.commit()

    def commit(self) -> None:
        """
        COMMIT;\n
        Hace commit de los cambios realizados en la base de datos
        """
        self.cursor.execute(f"COMMIT;")

    def insertColumnsWhereAND(self, table: str, listColumns: list[str], listValues: list, listParameters: list[str], listValuesParameters: list) -> None:
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

        for columnas, valores in zip(listColumns, listValues):
            columns += f"{columnas}, "
            valores = valores if type(valores) in [
                int, float] else f"\'{valores}\'"
            values += f"{valores}, "
        columns = columns[0:len(columns)-2]
        values = values[0:len(values)-2]

        for parametros, valoresParametros in zip(listParameters, listValuesParameters):
            parameters += f"{parametros}=\'{valoresParametros}\' AND "
        parameters = parameters[0:len(parameters)-5]

        query = f"INSERT INTO {table}({columns}) VALUES({values}) WHERE {parameters};"
        self.cursor.execute(query)
        self.commit()

    def insertColumnsWhereOR(self, table: str, listColumns: list[str], listValues: list, listParameters: list[str], listValuesParameters: list) -> None:
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
            valores = valores if type(valores) in [
                int, float] else f"\'{valores}\'"

            values += f"{valores}, "

        columns = columns[0:len(columns)-2]
        values = values[0:len(values)-2]

        for parametros, valoresParametros in zip(listParameters, listValuesParameters):
            valoresParametros = valoresParametros if type(valoresParametros) in [
                int, float] else f"\'{valoresParametros}\'"

            parameters += f"{parametros}={valoresParametros} OR "
        parameters = parameters[0:len(parameters)-4]

        query = f"INSERT INTO {table}({columns}) VALUES({values}) WHERE {parameters};"
        self.cursor.execute(query)
        self.commit()

    def insertRow(self, table: str, listValues: list) -> None:
        """
        INSERT INTO table VALUES(listValues);\n
        Se debe pasar a traves del parametro listColumns una lista de strings que contengan el nombre de las columnas
        donde se va a insertar la informacion pasada a traves del parametro listValues
        """
        values = ""

        for valores in listValues:
            valores = valores if type(valores) in [
                int, float] else f"\'{valores}\'"
            values += f"{valores}, "

        values = values[0:len(values)-2]

        query = f"INSERT INTO {table} VALUES({values});"
        self.cursor.execute(query)
        self.commit()

    def selectAllFrom(self, table: str) -> list[tuple]:
        """
        SELECT * FROM table; \n
        Selecciona todos los registros de una tabla
        """
        self.cursor.execute(f"SELECT * FROM {table};")
        return self.cursor.fetchall()

    def selectAllFromOrder(self, table: str, listColumn: list[str], order: str) -> list[tuple]:
        """
        SELECT * FROM table ORDER BY listColumn order;\n
        Selecciona todos los registros ordenados segun la lista de columnas,
        la variable order solo puede tomar dos valores ASC o DESC
        """
        columnas = ""
        for valores in listColumn:
            columnas += f"{valores}, "
        columnas = columnas[0:len(columnas)-2]

        query = f"SELECT * FROM {table} ORDER BY {columnas} {order};"
        self.cursor.execute(query)

        return self.cursor.fetchall()

    def selectAllWhereNull(self, table: str, columnParameter: str) -> list[tuple]:
        """
        SELECT * FROM table WHERE columnParameter IS NULL;\n
        Seleccionamos todos los valores de la table cuando la columnaParameter sea null
        """
        self.cursor.execute(
            f"SELECT * FROM {table} WHERE {columnParameter} IS NULL;")
        return self.cursor.fetchall()

    def selectAllWhereNotNull(self, table: str, columnParameter: str) -> list[tuple]:
        """
        SELECT * FROM table WHERE columnParameter IS NOT NULL;\n
        Seleccionamos todos los registros de la table cuando el columnParameter sea null
        """
        self.cursor.execute(
            f"SELECT * FROM {table} WHERE {columnParameter} IS NOT NULL;")
        return self.cursor.fetchall()

    def selectAllWhereAND(self, table: str, listColumnsParameters: list[str], listValuesParameters: list) -> list[tuple]:
        """
        SELECT * FROM table WHERE columnsParameters=valuesParameters AND ...;\n
        Selecciona todas las columnas de una tabla mientras se cumplan las condiciones, estas se concatenaran con AND
        """
        queryParameters = ""
        for parametros, valores in zip(listColumnsParameters, listValuesParameters):
            valores = valores if type(valores) in [
                int, float] else f"\'{valores}\'"
            queryParameters += f"{parametros}={valores} AND "
        queryParameters = queryParameters[0:len(queryParameters)-5]

        query = f"SELECT * FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def selectAllWhereOR(self, table: str, listColumnsParameters: list[str], listValuesParameters: list) -> list[tuple]:
        """
        SELECT * FROM table WHERE columnsParameters=valuesParameters OR ...;\n
        Selecciona todas las columnas de una tabla mientras se cumplan las condiciones, estas se concatenaran con OR
        """
        queryParameters = ""
        for parametros, valores in zip(listColumnsParameters, listValuesParameters):
            valores = valores if type(valores) in [
                int, float] else f"\'{valores}\'"
            queryParameters += f"{parametros}={valores} OR "

        queryParameters = queryParameters[0:len(queryParameters)-4]

        query = f"SELECT * FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def selectAllLikeAND(self, table: str, listColumnsParameters: list[str], listValuesParameters: list) -> list[tuple]:
        """
        SELECT * FROM table WHERE columnsParameters LIKE valuesParameters AND ...;\n
        Selecciona todas las columnas de una tabla mientras se cumplan las condiciones, 
        de busqueda parcial estas se concatenaran con AND
        """
        queryParameters = ""
        for parametros, valores in zip(listColumnsParameters, listValuesParameters):
            valores = valores if type(valores) in [
                int, float] else f"\'%{valores}%\'"
            queryParameters += f"{parametros} LIKE {valores} AND "

        queryParameters = queryParameters[0:len(queryParameters)-5]

        query = f"SELECT * FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def selectAllLikeOR(self, table: str, listColumnsParameters: list[str], listValuesParameters: list) -> list[tuple]:
        """
        SELECT * FROM table WHERE columnsParameters LIKE valuesParameters OR ...;\n
        Selecciona todas las columnas de una tabla mientras se cumplan las condiciones, 
        de busqueda parcial estas se concatenaran con OR
        """
        queryParameters = ""
        for parametros, valores in zip(listColumnsParameters, listValuesParameters):
            valores = valores if type(valores) in [
                int, float] else f"\'%{valores}%\'"
            queryParameters += f"{parametros} LIKE {valores} OR "

        queryParameters = queryParameters[0:len(queryParameters)-4]

        query = f"SELECT * FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def selectMin(self, column: str, table: str) -> list[tuple[float | int]]:
        """
        SELECT MIN(column) FROM table;\n
        Selecciona el valor mas pequeño de una columna de una tabla
        """
        self.cursor.execute(f"SELECT MIN({column}) FROM {table};")
        return self.cursor.fetchall()

    def selectMinWhereAND(self, column: str, table: str, listParameter: list[str], listValuesParameters: list) -> list[tuple[int | float]]:
        """
        SELECT MIN(column) FROM table WHERE listParameter=listValuesParameters AND...;\n
        Selecciona el valor mas pequeño de una tabla cuando se cumpla una o mas condiciones concatenadas con AND
        """
        condition = ""
        for parameters, values in zip(listParameter, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            condition += f"{parameters}={values} AND "
        condition = condition[0:len(condition)-5]

        query = f"SELECT MIN({column}) FROM {table} WHERE {condition};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def selectMinWhereOR(self, column: str, table: str, listParameter: list[str], listValuesParameters: list) -> list[tuple[int | float]]:
        """
        SELECT MIN(column) FROM table WHERE listParameter=listValuesParameters OR...;\n
        Selecciona el valor mas pequeño de una tabla cuando se cumpla una o mas condiciones concatenadas con OR
        """
        condition = ""
        for parameters, values in zip(listParameter, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            condition += f"{parameters}={values} OR "
        condition = condition[0:len(condition)-4]

        query = f"SELECT MIN({column}) FROM {table} WHERE {condition};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def selectMax(self, column: str, table: str) -> list[tuple[float | int]]:
        """
        SELECT MAX(column) FROM table;\n
        Selecciona el valor mas grande de la columna de una tabla
        """
        self.cursor.execute(f"SELECT MAX({column}) FROM {table};")
        return self.cursor.fetchall()

    def selectMaxWhereAND(self, column: str, table: str, listParameter: list[str], listValuesParameters: list) -> list[tuple[int | float]]:
        """
        SELECT MAX(column) FROM table WHERE listParameter=listValuesParameters AND...;\n
        Selecciona el valor mas grande de una tabla cuando se cumpla una o mas condiciones concatenadas con AND
        """
        condition = ""
        for parameters, values in zip(listParameter, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            condition += f"{parameters}={values} AND "
        condition = condition[0:len(condition)-5]

        query = f"SELECT MAX({column}) FROM {table} WHERE {condition};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def selectMaxWhereOR(self, column: str, table: str, listParameter: list[str], listValuesParameters: list) -> list[tuple[int | float]]:
        """
        SELECT MAX(column) FROM table WHERE listParameter=listValuesParameters OR...;\n
        Selecciona el valor mas grande de una tabla cuando se cumpla una o mas condiciones concatenadas con OR
        """
        condition = ""
        for parameters, values in zip(listParameter, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            condition += f"{parameters}={values} OR "
        condition = condition[0:len(condition)-4]

        query = f"SELECT MAX({column}) FROM {table} WHERE {condition};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def selectCount(self, table: str, column: str) -> int:
        """
        SELECT COUNT(column) FROM table;\n
        Cuenta cuantos registros hay de una columna de una tabla
        """
        self.cursor.execute(f"SELECT COUNT({column}) FROM {table};")
        aux = self.cursor.fetchall()
        return aux[0][0]

    def selectCountWhereAND(self, table: str, column: str, listParameters: list[str], listValuesParameters: list) -> int:
        """
        SELECT COUNT(column) FROM table WHERE listParameters=listValuesParameters AND...;\n
        Cuenta cuantos valores de una columna cumplen con ciertos parametros concatenados con AND
        """
        queryParameters = ""

        for parameters, values in zip(listParameters, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            queryParameters += f"{parameters}={values} AND "
        queryParameters = queryParameters[0:len(queryParameters)-5]

        query = f"SELECT COUNT({column}) FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        aux = self.cursor.fetchall()
        return aux[0][0]

    def selectCountWhereOR(self, table: str, column: str, listParameters: list[str], listValuesParameters: list) -> int:
        """
        SELECT COUNT(column) FROM table WHERE listParameters=listValuesParameters OR...;\n
        Cuenta cuantos valores de una columna cumplen con ciertos parametros concatenados con OR
        """
        queryParameters = ""

        for parameters, values in zip(listParameters, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            queryParameters += f"{parameters}={values} OR "
        queryParameters = queryParameters[0:len(queryParameters)-4]

        query = f"SELECT COUNT({column}) FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        aux = self.cursor.fetchall()
        return aux[0][0]

    def selectAverage(self, table: str, columnNumeric: str) -> float:
        """
        SELECT AVG(column) FROM table;\n
        Retorna el promedio de valores de una columna numerica de una base de datos
        """
        self.cursor.execute(f"SELECT AVG({columnNumeric}) FROM {table};")
        aux = self.cursor.fetchall()
        return aux[0][0]

    def selectAveragetWhereAND(self, table: str, columnNumeric: str, listParameters: list[str], listValuesParameters: list) -> int:
        """
        SELECT AVG(columnNumeric) FROM table WHERE listParameters=listValuesParameters AND...;\n
        Calcula el promedio de valores de una columna numerica si cumple las condiciones establecidas, 
        estas se concatenaran con el conector AND
        """
        queryParameters = ""

        for parameters, values in zip(listParameters, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            queryParameters += f"{parameters}={values} AND "
        queryParameters = queryParameters[0:len(queryParameters)-5]

        query = f"SELECT AVG({columnNumeric}) FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        aux = self.cursor.fetchall()
        return aux[0][0]

    def selectAveragetWhereOR(self, table: str, columnNumeric: str, listParameters: list[str], listValuesParameters: list) -> int:
        """
        SELECT AVG(columnNumeric) FROM table WHERE listParameters=listValuesParameters OR...;\n
        Calcula el promedio de valores de una columna numerica si cumple las condiciones establecidas, 
        estas se concatenaran con el conector OR
        """
        queryParameters = ""

        for parameters, values in zip(listParameters, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            queryParameters += f"{parameters}={values} OR "
        queryParameters = queryParameters[0:len(queryParameters)-4]

        query = f"SELECT AVG({columnNumeric}) FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        aux = self.cursor.fetchall()
        return aux[0][0]

    def selectSum(self, table: str, columnNumeric: str) -> int | float:
        """
        SELECT SUM(column) FROM table;\n
        Suma todos los valores de una columna numerica de una tabla
        """
        self.cursor.execute(f"SELECT SUM({columnNumeric}) FROM {table};")
        aux = self.cursor.fetchall()
        return aux[0][0]

    def selectSumtWhereAND(self, table: str, columnNumeric: str, listParameters: list[str], listValuesParameters: list) -> int:
        """
        SELECT SUM(columnNumeric) FROM table WHERE listParameters=listValuesParameters AND...;\n
        Suma todos los valores de una columna numerica que cumplan con las condiciones establecidas, estas se concatenaran 
        con el conector logico AND
        """
        queryParameters = ""

        for parameters, values in zip(listParameters, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            queryParameters += f"{parameters}={values} AND "
        queryParameters = queryParameters[0:len(queryParameters)-5]

        query = f"SELECT SUM({columnNumeric}) FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        aux = self.cursor.fetchall()
        return aux[0][0]

    def selectSumtWhereOR(self, table: str, columnNumeric: str, listParameters: list[str], listValuesParameters: list) -> int:
        """
        SELECT SUM(columnNumeric) FROM table WHERE listParameters=listValuesParameters OR...;\n
        Suma todos los valores de una columna numerica que cumplan con las condiciones establecidas, estas se concatenaran 
        con el conector logico OR
        """
        queryParameters = ""

        for parameters, values in zip(listParameters, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            queryParameters += f"{parameters}={values} OR "
        queryParameters = queryParameters[0:len(queryParameters)-4]

        query = f"SELECT SUM({columnNumeric}) FROM {table} WHERE {queryParameters};"
        self.cursor.execute(query)
        aux = self.cursor.fetchall()
        return aux[0][0]

    def updateWhereAND(self, table: str, listColumns: list[str], listValues: list, listParameters: list[str], listValuesParameters: list) -> None:
        """
        UPDATE table SET listColumns=listValues WHERE listParameters=ListValuesParameters AND ...;\n
        Metodo de actualizacion de campos, recibe como parametro el nombre de la tabla a modificar, 
        la lista de las columnas junto con una lista de sus valores nuevos, 
        tambien recibe como parametro para la condicion del where una lista de las columnas con otra con sus valores respectivos de bsuqueda encadenados con AND
        """
        set = ""
        for columnas, valores in zip(listColumns, listValues):
            valores = valores if type(valores) in [
                int, float] else f"\'{valores}\'"
            set += f"{columnas}={valores}, "
        set = set[0:len(set)-2]

        parameters = ""
        for parameter, valueParameter in zip(listParameters, listValuesParameters):
            valueParameter = valueParameter if type(valueParameter) in [
                int, float] else f"\'{valueParameter}\'"
            parameters = f"{parameter}={valueParameter} AND "
        parameters = parameters[0:len(parameters)-5]

        query = f"UPDATE {table} SET {set} WHERE {parameters};"
        print(query)
        self.cursor.execute(query)
        self.commit()

    def updateWhereOR(self, table: str, listColumns: list[str], listValues: list, listParameters: list[str], listValuesParameters: list) -> None:
        """
        UPDATE table SET listColumns=listValues WHERE listParameters=ListValuesParameters OR ...;\n
        Metodo de actualizacion de campos, recibe como parametro el nombre de la tabla a modificar, 
        la lista de las columnas junto con una lista de sus valores nuevos, 
        tambien recibe como parametro para la condicion del where una lista de las columnas con otra con sus valores respectivos de bsuqueda encadenados con OR
        """
        set = ""
        for columnas, valores in zip(listColumns, listValues):
            valores = valores if type(valores) in [
                int, float] else f"\'{valores}\'"

            set += f"{columnas}={valores}, "
        set = set[0:len(set)-2]

        parameters = ""
        for parameter, valueParameter in zip(listParameters, listValuesParameters):
            parameters = f"{parameter}=\'{valueParameter}\' OR "
        parameters = parameters[0:len(parameters)-4]

        query = f"UPDATE {table} SET {set} WHERE {parameters};"
        self.cursor.execute(query)
        self.commit()

    def deleteAll(self, table: str) -> None:
        """
        DELETE FROM table;\n
        Borra todos los datos de una tabla
        """
        self.cursor.execute(f"DELETE FROM {table};")
        self.commit()

    def deleteWhereAND(self, table: str, listParameters: list[str], listValuesParameters: list) -> None:
        """
        DELETE FROM table WHERE listParameters=listValuesParameters AND;\n
        Borra los registros de una tala cuando se cumplen los parametros, estos se concatenaran con AND
        """
        queryParameters = ""
        for parameters, values in zip(listParameters, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            queryParameters += f"{parameters}={values} AND "
        queryParameters = queryParameters[0:len(queryParameters)-5]

        query = f"DELETE FROM {table} WHERE {queryParameters};"

        self.cursor.execute(query)
        self.commit()

    def deleteWhereOR(self, table: str, listParameters: list[str], listValuesParameters: list) -> None:
        """
        DELETE FROM table WHERE listParameters=listValuesParameters OR ...;\n
        Borra los registros de una tala cuando se cumplen los parametros, estos se concatenaran con OR
        """
        queryParameters = ""
        for parameters, values in zip(listParameters, listValuesParameters):
            values = values if type(values) in [
                int, float] else f"\'{values}\'"
            queryParameters += f"{parameters}={values} OR "
        queryParameters = queryParameters[0:len(queryParameters)-4]

        query = f"DELETE FROM {table} WHERE {queryParameters};"

        self.cursor.execute(query)
        self.commit()
