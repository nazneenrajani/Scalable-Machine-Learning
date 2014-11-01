CPP = icpc
CFLAGS = -Wall -Wextra -Werror -O2 -std=c++0x -lstdc++ -openmp 
INC = -I eigen-eigen-1306d75b4a21\
      -I libigl/include

omp-als: src/omp-als.cpp
	$(CPP) $(CFLAGS) $(INC) src/omp-als.cpp ${LIBS} -o bin/omp-als

clean:
	rm bin/omp-als

test:
	bin/omp-als 20 1.0 16 data/large/

