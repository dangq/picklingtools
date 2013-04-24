
# Compare the relative speeds of serialing a pretty large table
# of PicklingProtocol 0, 1, 2 and "text" (use eval)
import copy
import cPickle as pickle
import sys

def CreateBigTable () :
    t = { }
    a = [ None, 1.0, 'hello', {}, {'a':1}, {'a':1, 'b':2}, [], [1], [1,2], [1,2,3]]
    
    for ii in xrange(0, 10000) :
        if ii%2==0 :
            t[str(ii)] = copy.deepcopy(a[ii%len(a)])
        else :
            t[ii] = copy.deepcopy(a[ii%len(a)])
    t["nested"] = copy.deepcopy(t)
    for ii in xrange(0, 10000) :
        t["AAA"+str(ii)] =  "The quick brown fox jumped over the lazy dogs quite a few times on Sunday "+str(ii);

    return t

if __name__ == "__main__" :
    times = 200
    if len(sys.argv) != 2 :
        print "Usage: python speed_test.py pickle0|unpickle0|pickle1|unpickle1|pickle2|unpickle2|unpickletext|pickletext"
        sys.exit(1)

    t = CreateBigTable()
    arg = sys.argv[1]
    
    if (arg=='unpickletext') :
        # Time how fast text parsing is
        big_str = str(t)

        for x in xrange(0, times) :
            loader = eval(big_str)
            # if loader != t : print("not same??")

    if (arg=='pickletext') :
        # Time how fast text parsing is
        for x in xrange(0, times) :
            big_str = str(t)
        
    elif arg in ["unpickle0", "unpickle1", "unpickle2"] :
        # Time host fast protocol 0 vs. 2 is
        proto = int(arg[-1])
        dumped = pickle.dumps(t, proto)
        
        for x in xrange(0,times):
            loader = pickle.loads(dumped)
            # if loader != t : print("not same??")
    elif arg in ["pickle0", "pickle1", "pickle2"] :
         proto = int(arg[-1])

         for x in xrange(0,times) :
             dumped = pickle.dumps(t, proto)
    else :
        print 'Unknown?'

