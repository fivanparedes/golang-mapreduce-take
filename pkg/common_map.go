package pkg

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // The name of the MapReduce job
	mapTaskNumber int, // Which map task this is
	inFile string, // File name of the input file.
	nReduce int, // The number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	//lee el archivo
	byteContent, err := ioutil.ReadFile(inFile)
	if err != nil {
		fmt.Println(err) //muestra el error
	}

	content := string(byteContent)

	//llama a map
	kvs := mapF(inFile, content)

	//crea archivos para guardar resultados del mapeo
	//make(map[string]*os.File) hace que las claves sean del tipo string y el valor de tipo struct File.
	//Por que **** un asterisco????
	fileMap := make(map[string]*os.File)

	for i := 0; i < nReduce; i++ {
		f, _ := os.Create(reduceName(jobName, mapTaskNumber, i)) //crea un archivo y lo almacena en la variable f
		defer f.Close()	//deja el cierre del archivo a lo ultimo
		fileMap[fmt.Sprintf("%d", i)] = f //mete en el diccionario fileMap el mismo archivo
	}

	for _, val := range kvs { //recorre el mapeo generado anteriormente
		hash := ihash(val.Key) //literalmente hace un hash con el valor del KeyValue llamado val
		partitionNum := hash % uint32(nReduce) //calcula el numero de particiones, devolviendo el resto entre el hash y el numero de tareas de reduccion
		enc := json.NewEncoder(fileMap[fmt.Sprintf("%d", partitionNum)]) //codifica a JSON el fileMap
		enc.Encode(&val) //codifica lo que hay en la direccion de memoria val
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
