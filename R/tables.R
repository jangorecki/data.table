
tryNrowUnique <- function(x, by=key(x)) tryCatch(nrow(unique(x=x, by=by)), error = function(e) NA_integer_) # workaround for data.table#1229

tryUniqueN <- function(x, by=key(x)) tryCatch(uniqueN(x=x, by=by), error = function(e) NA_integer_) # temp fix till data.table#1225 merged

MB = NCOL = NROW = UNQ_NROW = KEY_UNQ_NROW = TABLE_NAME = COLUMN_NAME = ORDER = IS_KEY = IS_INDEX = COUNT = COUNT_NA = RATIO_NA = NULL   # globals to pass NOTE from R CMD check

tables <- function(mb=TRUE,order.col="NAME",width=80,env=parent.frame(),silent=FALSE,pretty=TRUE,stats=FALSE)
{
    # Prints name, size and colnames of all data.tables in the calling environment by default
    tt = objects(envir=env, all.names=TRUE)
    ss = which(as.logical(sapply(tt, function(x) is.data.table(get(x,envir=env)))))
    if (!length(ss)) {
        if (!silent) cat("No objects of class data.table exist in .GlobalEnv\n")
        return(invisible(data.table(NULL)))
    }
    tab = tt[ss]
    info = data.table(NAME=tab)
    for (i in seq_along(tab)) {
        DT = get(tab[i],envir=env)   # doesn't copy
        set(info,i,"NROW",nrow(DT))
        set(info,i,"NCOL",ncol(DT))
        if (mb) set(info,i,"MB",ceiling(as.numeric(object.size(DT))/1024^2))  # mb is an option because object.size() appears to be slow. TO DO: revisit
        if(pretty){
            set(info,i,"COLS",paste(colnames(DT),collapse=","))
            set(info,i,"KEY",paste(key(DT),collapse=","))
        } else {
            set(info,i,"COLS",list(list(colnames(DT))))
            set(info,i,"KEY",list(list(key(DT))))
        }
        if(stats){
            set(info,i,"UNQ_NROW",tryNrowUnique(DT, by=NULL))
            set(info,i,"KEY_UNQ_NROW",tryNrowUnique(DT, by=key(DT)))
        }
    }
    if(pretty){
        info[,NROW:=format(sprintf("%4s",prettyNum(NROW,big.mark=",")),justify="right")]   # %4s is for minimum width
        info[,NCOL:=format(sprintf("%4s",prettyNum(NCOL,big.mark=",")),justify="right")]
        if(stats){
            info[,UNQ_NROW:=format(sprintf("%4s",prettyNum(UNQ_NROW,big.mark=",")),justify="right")]
            info[,KEY_UNQ_NROW:=format(sprintf("%4s",prettyNum(KEY_UNQ_NROW,big.mark=",")),justify="right")]
        }
    }
    if (mb) {
        total = sum(info$MB)
        if(pretty) info[, MB:=format(sprintf("%2s",prettyNum(MB,big.mark=",")),justify="right")]
    }
    if (!order.col %in% names(info)) stop("order.col='",order.col,"' not a column name of info") 
    info = info[base::order(info[[order.col]])]  # base::order to maintain locale ordering of table names
    if(silent){
        return(invisible(info))
    } else if(!pretty){
        return(info)
    } else if(pretty){
        m = as.matrix(info)
        colnames(m)[2] = sprintf(paste("%",nchar(m[1,"NROW"]),"s",sep=""), "NROW")
        colnames(m)[3] = sprintf(paste("%",nchar(m[1,"NCOL"]),"s",sep=""), "NCOL")
        if (mb) colnames(m)[4] = sprintf(paste("%",nchar(m[1,"MB"]),"s",sep=""), "MB")
        m[,"COLS"] = substring(m[,"COLS"],1,width)
        m[,"KEY"] = substring(m[,"KEY"],1,width)
        print(m, quote=FALSE, right=FALSE)
        if (mb) cat("Total: ",prettyNum(as.character(total),big.mark=","),"MB\n",sep="")
        return(invisible(info))
    }
}

columns <- function(env=parent.frame(),silent=FALSE,pretty=TRUE,stats=FALSE)
{
    tt = objects(envir=env, all.names=TRUE)
    ss = which(as.logical(sapply(tt, function(x) is.data.table(get(x,envir=env)))))
    if (!length(ss)) {
        if (!silent) cat("No objects of class data.table exist in .GlobalEnv\n")
        return(invisible(data.table(NULL)))
    }
    tab = tt[ss]
    info <- rbindlist(lapply(tab, function(tab){
        cols <- colnames(get(tab, envir=env))
        data.table(TABLE_NAME=rep(tab,length(cols)), COLUMN_NAME=cols)
    }))
    tab <- unique(info$TABLE_NAME) # removes data.table(NULL) from tab vector
    info[, ORDER := seq_along(COLUMN_NAME), TABLE_NAME]
    set2keyv(info, "TABLE_NAME")
    for (i in seq_along(tab)) {
        DT = get(tab[i],envir=env)
        irows = info[TABLE_NAME==tab[i], which=TRUE]
        set(info,irows,"TYPE",sapply(DT, typeof))
        info[irows, IS_KEY := COLUMN_NAME %chin% key(DT)]
        info[irows, IS_INDEX := COLUMN_NAME %chin% key2(DT)]
        if(pretty){
            set(info,irows,"CLASS",sapply(DT, function(col) paste(class(col),collapse=",")))
        } else {
            set(info,irows,"CLASS",list(lapply(DT, class)))
        }
        if(stats){
            set(info,irows,"COUNT",rep(nrow(DT),length(irows)))
            set(info,irows,"COUNT_UNQ",sapply(names(DT), function(colname) DT[,tryUniqueN(eval(as.name(colname)))]))
            set(info,irows,"COUNT_NA",sapply(names(DT), function(colname) DT[is.na(eval(as.name(colname))), .N]))
            info[irows, RATIO_NA := ifelse(COUNT_NA==0L, 0, COUNT_NA / COUNT)] # ifelse to handle 0 nrow dt which would produce NaN
        }
    }
    set2keyv(info,NULL)
    if(silent) invisible(info) else info[]
}
