package pkg

import "fmt"

//Estructura que me va a decir si el resultado fue OK
type Reply struct {
	OK bool
}

// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int // numero de tareas
	var nios int   // numero de entradas (para el reduce) o salidas (para map)
	switch phase { // bifurca la lectura/escritura segun la fase en la que se encuentra
	case mapPhase:
		ntasks = len(mr.Files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.Files)
	}

	fmt.Printf("Schedule: %v %v tareas (%d E/Ss)\n", ntasks, phase, nios)

	/* Todas las tareas tienen que ser programadas con los workers, y solamente cuando todos ellos fueron
	completados exitosamente, deberia retornar la funcion.
	Cada worker puede fallar, y cada worker puede completar varias tareas*/

	// Canales donde se envian las tareas pendientes
	taskChannel := make(chan *DoTaskArgs)
	doneChannel := make(chan bool)

	// registra el siguiente worker que trabajara en el mapreduce
	provideNextWorker := func() string {
		var worker string
		worker = <-mr.registerChannel

		return worker
	}

	// ejecuta la tarea y nos dice su estado
	runTask := func(worker string, task *DoTaskArgs) {
		var reply Reply
		ok := call(worker, "Worker.DoTask", task, &reply)
		if ok {
			doneChannel <- true
			mr.registerChannel <- worker
		} else {
			taskChannel <- task // vuelve a asignar la tarea si algo fallo
		}
	}

	// se crea una gorutina por cada tarea programada en el canal que asigna la tarea al worker
	go func() {
		for task := range taskChannel {
			worker := provideNextWorker()
			go func(task *DoTaskArgs) {
				runTask(worker, task)
			}(task)
		}
	}()

	// alterna los argumentos que se le pasan al worker para ejecutar la tarea
	go func() {
		for i := 0; i < ntasks; i++ {
			task := DoTaskArgs{mr.jobName, mr.Files[i], phase, i, nios}
			taskChannel <- &task
		}
	}()

	// manda al segundo canal las tareas terminadas
	for i := 0; i < ntasks; i++ {
		<-doneChannel
	}
	close(taskChannel) // cierra el canal de tareas si ya no hay nada pendiente

	fmt.Printf("Schedule: fase %v terminada.\n", phase)
}
