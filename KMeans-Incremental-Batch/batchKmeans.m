function [labels_n] = batchKmeans(X,k,tolerance)
[m,n]=size(X);
labels_n=randi(k,n,1);
t=0;
Q_old=0;Q_new=9999;
tic;
while abs(Q_old - Q_new) > tolerance
    t=t+1
    Q_old = Q_new;
    labels_prev = labels_n;
    
    C = zeros(m, k);
    S = zeros(m, k);
    
    for j = 1:k
        count = 0;
        for i = 1:n
            if labels_prev(i) == j
                S(:, j) = S(:, j) + X(:, i);
                count = count + 1;
            end
        end
        
        C(:, j) = S(:, j) / count;
    end
    
    for i = 1:n
        d=zeros(k,1);
        for j = 1:k
            d(j) = dot(C(:, j), X(:, i));
        end
        [~,ind] =max(d);
        labels_n(i)=ind;
    end
    
    Q_new = 0;
    for i = 1:k
        Q_new = Q_new + norm(S(:, i));
    end
end
toc;
