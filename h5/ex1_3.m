A = [
    -9 11 -21 63 -252;
    70 -69 141 -421 1684;
    -575 575 -1149 3451 -13801;
    3891 -3891 7782 -23345 93365;
    1024 -1024 2048 -6144 24572;
];


eigenvalue = [0 0 0 0 0];
singlevalue = [0 0 0 0 0];
eigenvalue = eigenvalue';
singlevalue = singlevalue';

matrice = [
    0 0 0 0 0;
    0 0 0 0 0;
    0 0 0 0 0;
    0 0 0 0 0;
    0 0 0 0 0;
];


for i = 1:1000
    error = eps(A);
    matrice = matrice + error;
    eigenvalue = eigenvalue + eig(A + error) - eig(A); 
    singlevalue = singlevalue + svd(A + error) - svd(A);
end

disp("2")
disp(eig(A));
disp(error/1000);

disp("2a")
disp(eigenvalue/1000)

disp("2b")
disp(singlevalue/1000)