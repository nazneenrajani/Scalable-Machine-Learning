function [labels_n] = incrementalKmeans(X,k,tolerance)
[m,n]=size(X);
labels_n=randi(k,n,1);
t=0;
labels_prev = labels_n;
C = zeros(m, k);
S = zeros(m, k);
for j = 1:k
    count = 0;
    for i = 1:n
        if (labels_prev(i) == j)
            S(:, j) = S(:, j) + X(:, i);
            count = count + 1;
        end
    end
    
    C(:, j) = S(:, j) / count;
end
Sim=zeros(n,k);
for i = 1:n
    for j = 1:k
        Sim(i,j) = dot(C(:, j), X(:, i));
    end
end
q=zeros(k,1);
Q_new = 0;
for i = 1:k
    q(i)=norm(S(:, i));
    Q_new = Q_new + q(i);
end
delta_star=999;
tic;
Cos=X'*X;
while abs(delta_star) > tolerance
    t=t+1
    labels_prev = labels_n;
   i_star=0;j_star=0;old_j=0;delta=0;
    for i = 1:n
        pi = labels_prev(i);
        if(pi==1)
            Q_pi=sqrt(q(1)^2-2*q(1)*Sim(i,1)+1);
            Q_2 = sqrt(q(2)^2+2*q(2)*Sim(i,2)+1);
            if(Q_pi+Q_2-(q(1)+q(2))>delta);
                delta=Q_pi+Q_2-(q(1)+q(2));
                i_star=i;j_star=2;old_j=1;
            end
            Q_3 = sqrt(q(3)^2+2*q(3)*Sim(i,3)+1);
            if(Q_pi+Q_3-(q(1)+q(3))>delta);
                delta=Q_pi+Q_3-(q(1)+q(3));
                i_star=i;j_star=3;old_j=1;
            end
        end
        if(pi==2)
            Q_pi=sqrt(q(2)^2-2*q(2)*Sim(i,2)+1);
            Q_1 = sqrt(q(1)^2+2*q(1)*Sim(i,1)+1);
            if(Q_pi+Q_1-(q(2)+q(1))>delta);
                delta=Q_pi+Q_1-(q(2)+q(1));
                i_star=i;j_star=1;old_j=2;
            end
            Q_3 = sqrt(q(3)^2+2*q(3)*Sim(i,3)+1);
            if Q_pi+Q_3-(q(2)+q(3))>delta;
                delta = Q_pi+Q_3-(q(2)+q(3));
                i_star=i;j_star=3;old_j=2;
            end
        end
        if(pi==3)
            Q_pi=sqrt(q(3)^2-2*q(3)*Sim(i,3)+1);
            Q_1 = sqrt(q(1)^2+2*q(1)*Sim(i,1)+1);
            if Q_pi+Q_1-(q(3)+q(1))>delta;
                delta=Q_pi+Q_1-(q(3)+q(1));
                i_star=i;j_star=1;old_j=3;
            end
            Q_2 = sqrt(q(2)^2+2*q(2)*Sim(i,2)+1);
            if Q_pi+Q_2-(q(3)+q(2)) > delta;
                delta=Q_pi+Q_2-(q(3)+q(2));
                i_star=i;j_star=2;old_j=3;
            end
        end
    end
   if old_j==0
       break;
   end
    q_pi =q(old_j) ;q_new =q(j_star);
    
    q(old_j)=sqrt(q(old_j)^2-2*q(old_j)*Sim(i,old_j)+1);
    q(j_star)=sqrt(q(j_star)^2+2*q(j_star)*Sim(i,j_star)+1);
    
    Sim(:,old_j)=(Sim(:,old_j)*q_pi - Cos(:,i_star))/q(old_j);
    Sim(:,j_star)=(Sim(:,j_star)*q_new+Cos(:,i_star))/q(j_star);
    
    delta_star=delta;
    labels_n(i_star)=j_star;
    Q_new =Q_new + delta_star
end
toc;
