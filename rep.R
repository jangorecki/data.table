cc()
x = c(NaN, 0, 0, -1, 0, 0, 0, 0, -1, 0, -Inf, 0, 1, 0, 1, 0, 1, 0, 
      1, 0, 0, 1, -1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, -1, 0, 0, 0, 0, 
      0, 0, 0, 0, -1, 0, 0, NA, 0, 0, 2, 0, NaN, -2, Inf, -1, 0, 0, 
      1, 0, -1, 0, -Inf, -1, 0, 0, -1, -1, 0, -1, -1, 0, 0, -2, 2, 
      0, 0, 0, 0, 0, 0, -2, 0, 0, 0, 0, 1, 0, 0, 0, -1, -1, Inf, 0, 
      0, 0, 0, 0, 1, 0, 0, 0, -1)
n = 10
a1 = frollmedian(x, n)
a2 = frollapply(x, n, median)
all.equal(a1, a2)
diff = which(a1!=a2)
cbind(diff, fm=a1[diff], fa=a2[diff])
#lapply(1:11, function(i) print(x[((i-1)*10+1):(i*10)])) -> nul

#x[diff]
#which(x==Inf)

q(status=0)

shellsort = function(x, has.na=NA){
  .Call(CshellsortR, as.double(x), as.logical(has.na))
}
rss = function(x) base::order(x, method="shell")

print(x)
for (i in 1:11) {
  y = x[((i-1)*10+1):min(i*10, 101)]
  o1 = shellsort(y)
  o2 = rss(y)
  stopifnot(isTRUE(all.equal(o1, o2)))
  if (!isTRUE(all.equal(o1, o2))) {
    cat("shellsort\n")
    print(o1)
    cat("R order\n")
    print(o2)
  }
}
