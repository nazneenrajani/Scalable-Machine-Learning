function[U,M] = als(lambda, k, iteration, R)

[u,m]=size(R);
U = rand(k,u);
M = rand(k,m);
[nrow,ncol,~] = find(R);
rn=unique(nrow);
cn=unique(ncol);

parfor i=1:u
    if(nnz(R(i,:))==0)
        U(:,i)=zeros(k,1);
    end
end

parfor i=1:m
    if(nnz(R(:,i))==0)
        M(:,i)=zeros(k,1);
    end
end

for iter = 1:iteration
    % Update M with fixed U
    for j=1:length(cn)
        [col,~,v]=find(R(:,cn(j)));
        Uk = U(:,col);
        M(:,cn(j))=(Uk*Uk'+lambda*eye(k))\(Uk*v);
        %M(:,cn(j))=cd_ridge(v,Uk',lambda);
    end
    
    % Update U with fixed M
    for i = 1:length(rn)
        [~,row,r]=find(R(rn(i),:));
        Mk = M(:,row);
        U(:,rn(i))=(Mk*Mk'+lambda*eye(k))\(Mk*r');
        %U(:,rn(i))=cd_ridge(r',Mk',lambda);
    end
end
end