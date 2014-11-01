clear;
small = load('hw1-data/small.mat');
R=small.R;
Rt=small.Rt;
lambda_v=[0.01 0.1 1];
for l=1:length(lambda_v)
    [U,M] = crossvalidation(R,lambda_v(l),10,10);
    disp(['Train RMSE = ' num2str(rmse(R,U'*M))]);
    disp(['Test RMSE = ' num2str(rmse(Rt,U'*M))]);
end