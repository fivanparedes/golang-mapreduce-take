package pkg

import (
	"encoding/json"
	"os"
	"strconv"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	//outFile string, //archivo de salida
	nMap int, // total de tareas de mapeo ejecutadas
	reduceF func(key string, values []string) string,
) {
	//Un diccionario donde la clave es un string y el valor es un slice de string
	var intermediary map[string][]string = make(map[string][]string)
	for m := 0; m < nMap; m++ {
		fr, _ := os.Open(reduceName(jobName, m, reduceTaskNumber)) //Si en el map creamos archivos, aca los abrimos uno por uno
		decoder := json.NewDecoder(fr)

	loop:
		for {
			var kv KeyValue //iteramos por cada par clave-valor encontrado en el archivo.
			err := decoder.Decode(&kv) //si la decodificacion no fue exitosa, se rompe el loop
			if err != nil {
				break loop
			}

			//construye los pares kv intermediarios
			if val, ok := intermediary[kv.Key]; !ok {  //confusa sintaxis, aparentemente busca que exista un valor y clave a la vez?
				arr := make([]string, 0)	//hace un slice de string
				arr = append(arr, kv.Value)	//mete en el slice el valor del par ordenado kv
				intermediary[kv.Key] = arr 	//mete el slice en la estructura intermediaria
			} else {
				val = append(val, kv.Value)	//mete el valor unico en la variable val
				intermediary[kv.Key] = val	//mete el val en la estructura intermediaria
			}
		}
	}

	out, _ := os.Create("mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTaskNumber)) //crea el archivo donde ira la reducción
	enc := json.NewEncoder(out)

	//realiza la reduccion
	for key := range intermediary {
		//codifica a json para luego ser combinado
		enc.Encode(&KeyValue{key, reduceF(key, intermediary[key])})
	}
	defer out.Close()
	
}
