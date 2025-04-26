#include "linked_list.h"

int ll_isNil(ll_pair p) {
    return p == NULL;
}

int ll_isEmpty(ll_pair b) {
  if (ll_isNil(b)) {
    return(1);
  } else {
    return(0);
  }
}



void* car(ll_pair l) {
  if (ll_isNil(l)) {
    printf("Cannot call car on empty list!\n");
    return(NULL);
  } else {
    if ( ll_isNil(l->car)) {
      return(NULL);
    } else {
      return(l->car);
    }
  }
}


//Building function cdr from line: 22

ll_pair cdr(ll_pair l) {
  if (ll_isEmpty(l)) {

    printf("Attempt to cdr an empty list!!!!\n");
    return(NULL);

  } else {
    return(l->cdr);
  };
}


//Building function cons from line: 32

ll_pair ll_cons(void* data, void* l) {
  ll_pair p = mega_malloc(hidden_pool, sizeof(ll_pair_s));
  p->car = data;
  p->cdr = l;
  return(p);
}


void ll_dump_string_list(ll_pair l) {
    for (;;) {
        if (!ll_isEmpty(l)) {
            printf("%s\n", l->car);
            l=l->cdr;
        } else {
            return;
        }
    }
}
