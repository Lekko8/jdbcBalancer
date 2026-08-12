package main

func main() {

	initDB()
	// TODO: добавить распределение нагрузки (по приоритетам, а при одинаковых - раунд робин)
	initPgBroker()

}
