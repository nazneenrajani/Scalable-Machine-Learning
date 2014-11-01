CPP = gcc
CFLAGS = -Wall -Wextra -Werror -O2 -g3 -std=c++0x -lstdc++ -fopenmp 
INC = -I eigen-eigen-1306d75b4a21
cd-svm: src/cd-svm.cpp
	$(CPP) $(CFLAGS) $(INC) src/cd-svm.cpp ${LIBS} -o bin/cd-svm

clean:
	rm bin/cd-svm

test:
	bin/cd-svm 0.1 1 data3/covtype.tr data3/covtype.t

