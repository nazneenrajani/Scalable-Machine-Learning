clear;
data = load('hw1-data/bodyfat.mat');
X=data.X;
y=data.y;
Xt=data.Xt;
yt=data.yt;
lambda_v=[10 1];
for l=1:length(lambda_v)
    w=cd_ridge(y,X,lambda_v(l));
    %w=cd_lasso(y,X,lambda_v(l));
    disp(['Lambda = ' num2str(lambda_v(l))]);
    trainerror = rmse(y,(X*w));
    disp(['Train RMSE = ' num2str(trainerror)]);
    testerror = rmse(yt,(Xt*w));
    disp(['Test RMSE = ' num2str(testerror)]);
end