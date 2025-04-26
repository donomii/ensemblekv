all: megapool
megapool_test.exe:
	gcc megapool.c -DII_WINDOWS -o megapool_test.exe
megapool_test:
	gcc megapool.c -o megapool_test
	./test
megapool.exe:
	gcc -m64 -O1 -Wl,--stack,16777216  main.c -DII_WINDOWS -Wall -Wno-format -Wno-unused-function -Wno-parentheses
megapool:
	gcc -o megapool -m64 -O1   main.c  -Wall -Wno-format -Wno-unused-function -Wno-parentheses -Wno-incompatible-pointer-types-discards-qualifiers
