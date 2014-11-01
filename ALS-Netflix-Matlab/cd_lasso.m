function w = cd_lasso(y,X,lambda)
[~,n] =size(X);
w = zeros(n,1);
for iter = 1:20
    for update=1:n
        temp1 = X*w-y;
        temp2 = X(:,update);
        delta=-w(update);
        if ((temp2'*temp1 - lambda)/(temp2'*temp2)) < w(update)
            delta = -(temp2'*temp1 + lambda)/(temp2'*temp2);
        end
        if ((temp2'*temp1 + lambda)/(temp2'*temp2)) > w(update)
            delta = -(temp2'*temp1 - lambda)/(temp2'*temp2);
        end
        w(update) = w(update)+delta;
    end
end
end