package main

func main() {
	buff := make([]byte, 100, 1024)

	subBuff1 := buff[10:15]
	copy(subBuff1, "12345")

	subBuff2 := buff [12:15]
	copy(subBuff2, "abc")


	copy(buff[500:], "123")
	println()
}
