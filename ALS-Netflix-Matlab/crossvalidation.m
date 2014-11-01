function [U,M] = crossvalidation(S,lambda,iteration,k)
error=0;
for cv =1:10
    idx = find(S);
    [p,q] =size(S);
    trainSize = fix(0.9*length(idx));
    trainRandomIdx = idx(randperm(length(idx),trainSize));
    testRandomIdx = setdiff(idx,trainRandomIdx);
    [trrow,trcol] = ind2sub(size(S),trainRandomIdx);
    [trow,tcol] = ind2sub(size(S),testRandomIdx);
  
    trainMatrix=sparse(trrow,trcol,S(trainRandomIdx),p,q);
    testMatrix=sparse(trow,tcol,S(testRandomIdx),p,q);
    
    [U,M] = als(lambda,iteration,k,trainMatrix);
    predicted = U'*M;
    error = error+rmse(testMatrix,predicted);
end
error=error/10;
disp(['RMSE = ' num2str(error)]);
end