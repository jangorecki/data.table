as.macro = function(...) {
    dots = match.call(expand.dots = FALSE)$`...`
    # make all arguments named explicitly
    dtq = match.call(match.fun(`[.data.table`), call = as.call(c(as.name("[.data.table"), as.name("x"), dots)))
    structure(as.list(dtq)[-(1:2)], class="macro")
}

if (testing <- FALSE) {
    N=5e3; K=10
    set.seed(1)
    DT = data.table(
        id1 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
        id2 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
        id3 = sample(K, N, TRUE),                          # large groups (int)
        id4 = sample(K, N, TRUE),                          # large groups (int)
        id5 = sample(N/K, N, TRUE),                        # small groups (int)
        v1 =  sample(5, N, TRUE),                          # int in range [1,5]
        v2 =  sample(round(runif(100,max=100),4), N, TRUE) # numeric e.g. 23.5749
    )
    options("datatable.macros" = TRUE)
    # aggregate queries
    count.ids = as.macro(, .N, by = c(paste0("id",1:5)))
    DT[~count.ids]
    count.unq = as.macro(, j = lapply(.SD, uniqueN))
    DT[~count.unq]
    sum.keyby = as.macro(, lapply(.SD, sum),, c(paste0("id",1:5)))
    DT[~sum.keyby]
    v2.sum.keyby = as.macro(, lapply(.SD, sum),, c(paste0("id",1:5)), .SDcols ="v2")
    DT[~v2.sum.keyby]
    # join queries
    ref = data.table(id1=c("id001","id002"), id3=1:4)
    sum.byref = as.macro(ref, lapply(.SD, sum), .EACHI, on=names(ref), .SDcols=c("v1","v2"))
    DT[~sum.byref]
    # agg col queries
    id1.v2 = as.macro(, id1.v2.total := sum(v2), by = id1)
    DT[~id1.v2][]
    # update queries
    upd.id1.v2 = as.macro(, id1.v2.total := mean(v2), by = id1)
    DT[~upd.id1.v2][]
    # update queries sub-assign
    top3na = as.macro(1:3, id1.v2.total := NA)
    DT[~top3na][]
    # add col queries on join
    ref = data.table(id1=c("id001","id002","id003"), val = 2:4)
    upd.col.jn = as.macro(ref, val := i.val, on = "id1")
    DT[~upd.col.jn][]
    # update queries on join
    ref = data.table(id1=c("id001","id002"), val = 50L)
    DT[~upd.col.jn][]
    # drop column
    # drop.cols = as.macro(, c("id1.v2.total","val") := NULL)
    # DT[~drop.cols][]
    # chaining
    by.1234 = as.macro(, .(v2=sum(v2)), c(paste0("id",1:4)))
    by.123 = as.macro(, .(v2=sum(v2)), c(paste0("id",1:3)))
    by.12 = as.macro(, .(v2=sum(v2)), c(paste0("id",1:2)))
    by.1 = as.macro(, .(v2=sum(v2)), id1)
    DT[~by.1234]
    DT[~by.1234][~by.123]
    all.equal(
        DT[~by.1234][~by.123][~by.12][~by.1],
        DT[, .(v2=sum(v2)), id1]        
    )
    DT[~macronotexists]
    badmacro = list(i = 1:2)
    DT[~badmacro]
    class(badmacro) = "macro"
    DT[~badmacro] # now is OK
}
