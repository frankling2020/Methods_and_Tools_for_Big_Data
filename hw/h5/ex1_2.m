clear;
clearvars;

sz = 100;

matrices = rand(sz, 1000, 100);
disp("(a).")
tic;
for i = 1:sz
    [U, D, V] = svd(matrices(i));
end
toc;


disp("(b).")
tic;
for i = 1:sz
    [U, D, V] = svd((matrices(i))');
end
toc;

disp("(c).")
tic;
for i = 1:sz
    eigen = eig(matrices(i) * (matrices(i))');
end
toc;

disp("(d).")
tic;
for i = 1:sz
    eigen = eig((matrices(i))' * matrices(i));
end
toc;