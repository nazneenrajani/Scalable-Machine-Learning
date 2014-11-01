CPP = gcc
CFLAGS = -Wall -Wextra -Werror -O2 -g3 -std=c++0x -lstdc++ -fopenmp 
cd-svm: src/m.cpp
	$(CPP) $(CFLAGS) $(INC) src/m.cpp ${LIBS} -o bin/m

clean:
	rm bin/m

test:
	bin/m data/matrix

