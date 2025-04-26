
#include <string.h>
#include <stdio.h>

/*
 * To use this function, initialize a pointer P 
 * to point to the start of the string. Then extract
 * tokens T like this:
 * T = get_next_token(&P, delimiters);
 * When it returns a null pointer, there are no more,
 * and P is set to null value as well.
 */

char *get_next_token(char **context, const char *delim)
{
  char *ret;

  /* A null context indicates no more tokens. */
  if (*context == 0)
    return 0;
 
  /* Skip delimiters to find start of token */
  ret = (*context += strspn(*context, delim));

  /* skip to end of token */
  *context += strcspn(*context, delim);

  /* If the token has zero length, we just
     skipped past a run of trailing delimiters, or
     were at the end of the string already.
     There are no more tokens. */

  if (ret == *context) {
    *context = 0;
    return 0;
  }

  /* If the character past the end of the token is the end of the string,
     set context to 0 so next time we will report no more tokens.
     Otherwise put a 0 there, and advance one character past. */

  if (**context == 0) {
    *context = 0;
  } else {
    **context = 0;
    (*context)++;
  }

  return ret;
}

/*
 * Handy macro wrapper for get_next_token
 */

#define FOR_EACH_TOKEN(CTX, I, S, D) \
    for (CTX = (S), (I) = get_next_token(&(CTX), D); \
	 (I) != 0; \
	 (I) = get_next_token(&(CTX), D))

int test_tokeniser(int argc, char **argv)
{
  char *context, *iter; 

  if (argc >= 2)
    FOR_EACH_TOKEN (context, iter, argv[1], ":")
      puts(iter); 

  return 0;
}

int tokenise_path(char *input)
{
  char *context, *iter; 

  
    FOR_EACH_TOKEN (context, iter, input, "/\\")
      puts(iter); 

  return 0;
}