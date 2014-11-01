function error = rmse(actual,predicted)
[~,q] = size(actual);
if q==1
    error = sqrt(mean((actual-predicted).^2));
else
    [r,~,act] = find(actual(:));
    temp = predicted(:);
    pred = zeros(length(r),1);
    for i=1:length(r)
        pred(i) = temp(r(i));
    end
    error = sqrt(mean((act-pred).^2));
end
end