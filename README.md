# m4

Running the program requires 3 seperate terminals.

From root run the following,
AND wait 5 seconds after the FINAL terminal has started.

##### TERMINAL 1
`go run processes/node.go --id=1`

##### TERMINAL 2
`go run processes/node.go --id=2`

##### Terminal 3
`go run processes/node.go --id=3`

THEN to make a processes request access to the CRITICAL SECTION 
simply move to that particular terminal window and press `ENTER`

WHEN a process enters the CRITICAL SECTION they stay there for 3 seconds.

TO TERMINATE AND EXIT move into EACH TERMINAL and press `ctrl+c`