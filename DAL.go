package godal

import (
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	// CONTROLADOR PARA CONEXIONES CON MYSQL
	_ "github.com/go-sql-driver/mysql"
)

//Conexion Datos para la conexion
type Conexion struct {
	Servidor string
	Puerto   string
	BD       string
	Usuario  string
	Clave    string
}

//DAL capa de acceso a datos (data access layer)
type DAL struct {
	Estado         bool
	DB             *sql.DB
	TX             *sql.Tx
	STMT           *sql.Stmt
	UltimoID       int64
	FilasAfectadas int64
	errores        errores
}

//Errores Lista de errores generados
type errores []error

//Error Informacion del error
type Error struct {
	Codigo   string
	Mensaje  string
	Detalles error
	Archivo  string
	Funcion  string
	Linea    int
}

//FilaSQL estructura para mapeo de datos obtenidos
type FilaSQL map[string]string

var erroresSQL = map[string]string{
	"0":    "ERROR DESCONOCIDO",
	"1045": "USUARIO O CONTRASEÑA INCORRECTOS",
	"1049": "BASE DE DATOS DESCONOCIDA",
	"1451": "NO SE PUEDE BORRAR PORQUE ESTA REFERENCIADA EN OTRA TABLA",
	"1064": "ERROR EN LA SINTAXIS DEL SQL",
	"tcp":  "No se puede establecer una conexión ya que el equipo de destino denegó expresamente dicha conexión",
}

func obtenerCodigoError(err error) string {
	errString := err.Error()
	parte1 := strings.Split(errString, ":")
	if len(parte1) > 1 {
		parte2 := strings.Split(parte1[0], " ")
		if len(parte2) == 2 {
			return parte2[1]
		}
	}
	return "0"
}

func traducirCodigoError(codigo string) string {
	traduccion := erroresSQL[codigo]
	if traduccion == "" {
		codigo = "0"
	}
	return erroresSQL[codigo]
}

func crearError(err *Error) {
	if err.Detalles != nil {
		var pc uintptr
		pc, err.Archivo, err.Linea, _ = runtime.Caller(1)
		err.Funcion = runtime.FuncForPC(pc).Name()
		err.Codigo = obtenerCodigoError(err.Detalles)
		err.Mensaje = traducirCodigoError(err.Codigo)
	}
}

//Conectar Establece conexion con el servidor de base de datos y retorna una struct DAL
func (c Conexion) Conectar() (dal DAL, err Error) {
	dsn := c.Usuario + ":" + c.Clave + "@tcp(" + c.Servidor + ":" + c.Puerto + ")/" + c.BD + "?parseTime=true&loc=America%2FBogota"
	dal.DB, err.Detalles = sql.Open("mysql", dsn)
	if err.Detalles != nil {
		crearError(&err)
		return dal, err
	}
	err.Detalles = dal.DB.Ping()
	dal.Estado = err.Detalles == nil
	crearError(&err)
	return dal, err
}

//LiberarConexion cierra la conexion actual
func (d DAL) LiberarConexion() error {
	fmt.Println("LIBERANDO CONEXION")
	return d.DB.Close()
}

func (d *DAL) esTransaccion() bool {
	return d.TX != nil
}

func (d *DAL) esSentenciaPreparada() bool {
	return d.STMT != nil
}

//PrepararSentencia Prepara la sentencia sql y retorna puntero a la sentencia
func (d *DAL) PrepararSentencia(query string) (err Error) {
	if d.esTransaccion() {
		d.STMT, err.Detalles = d.TX.Prepare(query)
	} else {
		d.STMT, err.Detalles = d.DB.Prepare(query)
	}
	crearError(&err)
	return err
}

//LiberarSentencia cierra la sentencia preparada
func (d *DAL) LiberarSentencia() error {
	fmt.Println("LIBERANDO SETENCIA")
	return d.STMT.Close()
}

//ObtenerFilas Obtiene las filas que genere la consulta SQL dada o preparada
func (d *DAL) ObtenerFilas(query string, valores ...interface{}) (tabla []FilaSQL, err Error) {
	var filas *sql.Rows
	if d.esSentenciaPreparada() {
		filas, err.Detalles = d.STMT.Query(valores...)
	} else {
		filas, err.Detalles = d.DB.Query(query, valores...)
	}
	if err.Detalles != nil {
		crearError(&err)
		return nil, err
	}
	defer filas.Close()
	var columns []string
	columns, err.Detalles = filas.Columns()
	if err.Detalles != nil {
		crearError(&err)
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))

	for c := range values {
		scanArgs[c] = &values[c]
	}

	for filas.Next() {
		err.Detalles = filas.Scan(scanArgs...)
		if err.Detalles != nil {
			crearError(&err)
			return nil, err
		}
		fila := make(FilaSQL, len(columns))
		for i, val := range values {
			if val == nil {
				fila[columns[i]] = "--"
			} else {
				fila[columns[i]] = string(val)
			}
		}
		tabla = append(tabla, fila)
	}
	return tabla, err
}

//EjecutarSentencia ejecuta sentencia sql
func (d *DAL) EjecutarSentencia(query string, values ...interface{}) (err Error) {
	var resultado sql.Result
	if d.esSentenciaPreparada() {
		resultado, err.Detalles = d.STMT.Exec(values...)
	} else {
		if d.esTransaccion() {
			resultado, err.Detalles = d.TX.Exec(query, values)
		} else {
			resultado, err.Detalles = d.DB.Exec(query, values)
		}
	}
	if err.Detalles == nil {
		d.UltimoID, err.Detalles = resultado.LastInsertId()
		d.FilasAfectadas, err.Detalles = resultado.RowsAffected()
	} else {
		d.errores = append(d.errores, err.Detalles)
	}
	crearError(&err)
	return err
}

//IniciarTransaccion Inicia una transaccion
func (d *DAL) IniciarTransaccion() (err Error) {
	d.TX, err.Detalles = d.DB.Begin()
	crearError(&err)
	return err
}

//FinalizarTransaccion finalizar la transaccion
func (d *DAL) FinalizarTransaccion() (commit bool, err Error) {
	if len(d.errores) > 0 {
		err.Detalles = d.TX.Rollback()
	} else {
		err.Detalles = d.TX.Commit()
		commit = true
	}
	crearError(&err)
	return commit, err
}
