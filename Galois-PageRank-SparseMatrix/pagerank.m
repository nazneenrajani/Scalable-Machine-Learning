A=load('hw4-data.mat');
  A=A.A;
    D=sum(A);
     D=D';
     node=1770961;
     r=ones(node,1);
     r=r/node;
     p=ones(node,1);
      D=p./D;
      tic;
      for iter=1:50
	      q=0.85*D.*r;
	      r=A*q+(0.15/node);
      end
      toc;
