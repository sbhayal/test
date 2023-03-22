static char stream_c_id[]="@(#) Stream/stream.c 2.1 (08/12/98) by Tom Henderson (TIMS) Build: 02/19/99";

/* stream.c 
 *
 * This program is designed to never by started interactively.  
 *
 * To run it: stream FIPS ss_file
 *   Where ss_file is a "streams script" for execution.  
 *   (Start_proc.c uses the file "$TALOG/FIPS.d/tmp.ss")
 *
 * Error messages go to the file $TALOG/FIPS.d/stream.log if possible,
 * or to $HOME/ERROR if not, or to /tmp/ERROR as a last resort.
 *
 * As processes start/finish entries are made in $TALOG/FIPS.d/stream.log
 * file.  A lock is put on the file $TALOG/.$FIPS.lck to prevent
 * multiple invocations of this stream with the same $TALOG.  NOTE,
 * however, EVEN A SEPERATE $TALOG will NOT PREVENT CONFLICTS IN THE
 * PROGRAMS stream RUNS.  Most programs read/write files in $TAWORK.
 * The reason the file is locked is written in this file.
 * Currently: Being Configured by <logname>
 *            Being Run by <logname>
 *
 * Any existing stream.log3 file is deleted, stream.log2 is renamed 
 * stream.log3 ... and stream.log copied to stream.log1.  stream.log 
 * is then truncated.
 *
 * A output file named "stream.out" is created, in the $TALOG/FIPS.d
 * directory, and stderr and stdout are closed and reopened to point
 * to this file, in case programs produce output. (Though they shouldn't)
 * stdin is closed (the input channel).
 *
 * Any stream2.out file is deleted, stream1.out is moved 
 * stream2.out ... and stream.out moved to stream0.out.  
 *
 * The program becomes a new group leader, detatches from the terminal, 
 * forks, parent exits.  The child runs programs from the script.
 * containing programs to run, in a particular order.  
 * Then the process forks(), parent dies making child a daemon.  The 
 * process connects stdout and stderr to  a log file, and stdin
 * to /dev/null, and detatches from the terminal by becoming a
 * new process group and group leader.  It then starts all processes
 * in a step, and in turn, each "sub-process" on a channel until
 * the step is completed.  Logging of processes started, and 
 * return codes are put into a log file, separate from the 
 * stdout & stderr log file.
 *
 * Parallel processing is supported by specifying the "channel"
 * for a process to run on.  All processes for a specific step
 * are started in parallel - 1 for each "channel".  A channel
 * has a "process" number, so several steps can run on a channel
 * in order, and will complete before the next step can start.
 *
 * To use this process, programs need to return a zero completion
 * code for success, and have no user interaction.  Any generated
 * output will go to the above mentioned log file.  
 *
 * The format of the "ss" file is:
 *
 * Fields are separated by ~.
 * Empty lines or lines starting with a # are ignored
 * File must be sorted by step number, and process number within channel
 * number.
 * If the last field ends with a ~, trailing comments are permitted
 * fields:
 * step [ 1 - n, need not be sequential, though must be in numerical order ]
 * channel [ 0 - TOTALCHANNELS]  These are ABSOLUTE channel numbers
 * process [ 1 - n, need not be sequential, though must be in numerical order ]
 * comment Any letters (except `) describing the step for a user
 * command The command to run. (Remember i/o redirection is taken care of)
 * An optional comment
 *
 * Sample:
 *
 * 100~0~10~First step~/bin/ls~  This is an optional comment on the line
 * 200~0~10~One of two processes, step 2~/bin/who~  And another comment
 * 200~1~10~First Two of two processes, step 2~/bin/who~  And another comment
 * 200~1~20~Second Two of two processes step 2~/bin/who~  And another comment
 * 300~0~10~Last Process~/bin/who~  And another comment
 *
 *    Channel 0               Channel 1              Channel 2
 *  ====================== ====================== ======================
 * | First Step           |                      |                      |
 * | One of two processes | First two of two proc|                      |
 * |                      | Second Two of two    |                      |
 * | Last Process         |                      |                      |
 *
 *
 * 12/21/1993 In Development by Tom Henderson
 *  Contains debug code for testing calls to fork() ,etc
 * 12/22/1993
 *  Testing looks OK. Put real fork() and wait() calls in, and test.
 * 12/22/1993
 *  Now I put the real fork(), exec() and wait() calls in...
 * 03/07/1994:
 *  Now allow user to have non-sequential, (though still in sorted order)
 *  step number in ".ss" script for step and process numbers
 * 03/14/1994:
 *  Output files are put in $TALOG/FIPS.d directory.
 * 06/09/1994:
 *  Added signal handling.  Upon receipt of signal, we set to ignore
 *  further receipt of the signal  - we will then signal all children 
 *  in our process group.  We also set a global flag, start_no_more,
 *  to prevent starting additional processes.
 *  We will eventually exit because the process on each channel will 
 *  exit (with an error code indicating exit due to a signal), resetting 
 *  the "last_step" to the current step.  
 *  If the signal comes in when not doing a wait(), various oddities
 *  could happen, as the return from the signal handler won't 
 *  return to the middle of certain system calls.  We handle the most
 *  likely case, the wait(), by doing a continue if we exited due to
 *  EINTR, which loops back to the wait() again to catch the children
 *  exiting from their receipt of the signal generated by the signal
 *  handler routine.  There is a small window when there may be no
 *  children running (Initially, before any processes have started,
 *  and after a process has stopped, but before another has started)
 *  that will cause the program to get to the wait with no children.
 *  If a child process ignores the SIGTERM, this program will keep
 *  running, waiting for the child to exit.
 *
 * 06/10/1994:
 *  Implement getopts(), and add email using the -e option.
 *  Using the -e option, mail will be sent to those requested upon
 *  completion of the stream, if stream can get started OK.
 *  (i.e., open the logging file, get a file lock, etc.)
 *  NOTE: The stream might not exit through the done() routine, which
 *        sends the mail.  A signal such as -9 will prevent exit through
 *        the done() routine, and in the beginning, stream may exit
 *        through the TAhome_error() call.
 *
 *        TAhome_error() ALWAYS sends email to the USER, however,
 *        telling them to see their ~/ERROR file.  This action does not
 *        depend upon, or utilize, the -e <mail_to_name> parameter.
 *
 *  NOTE: with PAUSE or STOP, the program could still exit "normally"
 *        if it was already on the "last" step...
 *
 * 06/15/1994:
 *  Implement fork() error handling - no way to really test this code,
 *  though, as if would affect all users on our system...
 *
 * 06/17/1994: TCH
 *  Exit codes under 10 are user problems, in general.
 *  Exit codes above 10 are system() problems.
 *   0-NORMAL EXIT
 *   1-Non-zero return code from program exit (error or signal), or
 *     exec of program failed,  (EXIT will be 69 for the child.) 
 *   2-.ss file line parse error (field count, etc.)
 *   3-.ss file line "command field" parese error. (too many parms., etc.)
 *   4-Couldn't start a program, possible because fork() failed.
 *   5-Signal received (and no programs were running) so wait() failed.
 *     (A small window when this could happen, highly unlikely though.
 *     usually a program will be running, and signal will cause it to
 *     exit with a "SIGNAL", which causes stream to stop...)
 *
 * 06/17/1994: TCH
 *   TAshuffle() for .log and .out files changed from 2 to 9 to leave
 *   more history of stream processing, at Clare's request.
 */

/* SETUID to "sysop" so signal can be sent to cause a PAUSE
 * scripts have STEP, CHANNEL, PROCESS 
 * build in catch of signal to do pause, update log record, and wait...

 * look at way to pause...let processes finish, but don't fork
 * on last wait, if pauseded, just sleep() or pause() ?
 */

#define FIELDSIZE  128  /* size of fields in array & file name strings */
#define MAX_FORKS    6  /* number of forks() to try, before giving up */
#define FORK_DELAY 300  /* number of seconds to delay before re-trying fork() */
#define PAUSE_DELAY 60  /* number of seconds to pause, before re-checking */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <string.h>
//#include <strings.h>
#include <pwd.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <pwd.h>
#include <libgen.h>
#include <logp.h>
#include <datagroupmgr.h>
#include <btstring.h>
#include "TAstream.h"

#ifndef TRUE
#define	TRUE     (1)	/* logical true */
#endif

#ifndef FALSE
#define	FALSE    (0)	/* logical false */
#endif

#define SBASE 1  /* the number for the first step:
                  * Should not be zero - under CONTROL in
                  * stream-mgr we let the user
                  * reset the "last_stop" to 0 to force a graceful
                  * stop of processing, as soon as current jobs finish.
                  */
#define PBASE 1  /* the number for the first process on a channel */

/* GLOBAL DEFINITIONS */

static char *progname;      /* from argv[0] */
static char *global_datagroup;   /* from -f parameter */
static time_t StartTime;    /* finish message in done() now logs duration */

static FILE *log_fp;        /* file pointer of the log file */
//static int lock_fd;         /* lockfile_name file descriptor */
//static char lockfile_name[128]; /* name of lock file - used to unlink it */
static int TIMS_log=0;      /* by default do not do TIMS log routine */

static struct sline {
   int step;                /* Sequential step number, e.g. SBASE, SBASE+1,...*/
   int u_step;              /* the users step number, e.g.,100, 200, etc. */
   int channel;             /* 0 - TOTALCHANNELS */
   int process;             /* Sequencial process number, e.g. PBASE, PBASE+1 */
   int u_process;           /* the users process number, e.g. 10, 20, 30, ... */
   char desc[FIELDSIZE];    /* description of the process to run */
   char command[FIELDSIZE]; /* the command to fork() and exec() */
   pid_t pid;               /* the pid of the command */
   time_t start;            /* time the job started */
   time_t stop;             /* time the job finished */
   int exit_status;         /* exit status of the command */
   } line[MAXSCRIPTLINES];

static int line_count;      /* number of members in line structure */
static int start_no_more=0; /* 1=received signal
                             * 2=pause command received
                             * 3=stop command received
                             */

static struct sigaction term_act;     /* "terminate" signal action */
static char *email_to=NULL;           /* passed as arg to -e parameter */
static char email_datagroup[256]="Not_Set"; /* used in subj. when done() sends mail */

/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
/* This process makes notifications in the error log file 
 */

static void log_entry(char *message)
   {   /* the message line to display */
   
   extern void TAhome_error(const char *, const char *);
   time_t ticks;  /* number of ticks on the clock for use getting date stamp */
   char buffer[256];
   
   ticks=time(NULL);                                             /* get time */
   if((fprintf(log_fp,"+ %s%s\n",ctime(&ticks),message)) == -1) {
      sprintf(buffer,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,buffer);
      }
   else fflush(log_fp);
   }
/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
static void do_tims_log(
   int error_num,      /* My choice of an error number */
   int rec_type,       /* LGPROSTART, LGPROEND,LGCOMMENT */
   char *comment)  {   /* Any comment I want not ending in \  */

   LOGSTRUCT tlog;

   tlog.i1=LGWRITE;   /* This means Write A Log record */
   tlog.i2=error_num; /* Not really used for LGPROSTART or LGPROEND? */
   tlog.rt=rec_type;  /* We use start, end, or a comment type */
   tlog.context=0;    /* For TIMS "modules" that fail in your program */
                      /* I don't call any TIMS modules, except this one. */
   /* pid, ppid, uid, ltime & ldate are filled in in logproc() 
      (so why are they in the structure?)
    */
   strcpy(tlog.datagroupid, global_datagroup);
   strcpy(tlog.progid, basename(progname));
   strcpy(tlog.s1, comment);
   logproc(&tlog);    /* no reason to check return code - can't do anything */
                      /* if this fails... */
   }
/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
static void done(
   char * message,
   int status)      {
   
   time_t FinishTime;
   time_t duration;
   time_t hours; 
   time_t minutes; 
   time_t seconds; 
   char Sduration[20];  /* string (duration nn:nn:nn) */
   char temp[256];
   char temp_msg[256];
   char cmd[256];
   char subject[512];
   pid_t pid;
   char * streamDoneArgs[5];
   char * p;
   extern int TAemail(char *, char *, char *);         /* from libstream.c */
   /* if we exit with children, they will become orphans. */

   /* Contact the Web Service to send an email */
   streamDoneArgs[0] = "streamDone.sh";
   streamDoneArgs[1] = email_datagroup;
   if(status) {
     streamDoneArgs[2] = "FAILURE";
   } else {
     streamDoneArgs[2] = "SUCCESS";
   }

   sprintf(temp_msg,"DONE: %s (Exit:%d)",message, status); 
   streamDoneArgs[3] = temp_msg;
   streamDoneArgs[4] = NULL;

   if((p=getenv("RIDE_DGPATH"))!=NULL ) {
     sprintf( cmd, "%s/streamDone.sh", p );
     sprintf( temp, "Calling %s %s %s", cmd, streamDoneArgs[1], streamDoneArgs[2] );
     log_entry(temp);
     pid = fork(); 
     if(!pid) {
  //      (void) execvp( "/export/home/cptest1/streamDone.sh", streamDoneArgs );
        (void) execvp( cmd, streamDoneArgs );
        sprintf( temp, "exec() failed to call streamDone.sh" );
        log_entry(temp);
        exit(69); /* exec() failed, exit with an error code to notify parent */
     }
   } else {
     sprintf( temp, "RIDE_DGPATH not set, not sending email" );
     log_entry(temp);
   }
  

   /* if user requested email upon completion - do it HERE */
   if(email_to) {       /* user requested email with -e command line option */
      sprintf(subject,"Stream for %s is done", email_datagroup);
      sprintf(temp,"%s\nExit status of %d.\n",message,status);
      if(TAemail(email_to, subject, temp)) {
         sprintf(temp,"TAemail(%s,%s,<message>) failed.", email_to, subject);
         log_entry(temp);
         }
      else {
         sprintf(temp,"EMAIL: sent to %s", email_to);
         log_entry(temp);
         }
      }
   
   sprintf(temp,"STATE: DONE: %s (Exit:%d)",message, status); /*final message */
   log_entry(temp);
   if(TIMS_log) {
      time(&FinishTime);  
      duration = FinishTime-StartTime;
      hours = duration / (60*60);
      duration -= (hours * (60*60));
      minutes = duration / 60;  
      duration -= minutes * 60;
      seconds = duration;
      sprintf(Sduration,"%ld:%02ld:%02ld", hours, minutes, seconds);
      sprintf(temp,"Finished(%s). %s",Sduration, message); 
      do_tims_log(status,LGPROEND,temp); /* finishing */
      }
   (void) fclose(log_fp);
//   (void) close(lock_fd);  /* this also removes the lock */
/* unlink any "ctrl" files that may have been set too late to do any good. */
//   unlink(lockfile_name);
   (void) close(STDOUT_FILENO);  /* flush the buffers to the output file */ 
   (void) close(STDERR_FILENO); 
   exit(status); /* parent is "init", and no one really cares about "status" */
   }

/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
static void sig_handler(int sig_id)  { 
       // NOTE: since this routine is used as a signal handler, it must
       // take sig_id as a parameter
   char temp[256];
   term_act.sa_handler=SIG_IGN; /* Now ignore further receipt of this signal */
   if(sigaction(SIGTERM,&term_act,NULL) == -1) {   /* and set the new action */
      sprintf(temp,"ERROR: sigaction() to ignore signal failed: %s", 
      strerror(errno));
      done(temp,34);
      }
   sprintf(temp,"NOTICE: Received SIGTERM signal. Stopping all processes.");
   log_entry(temp);
   start_no_more=1;      /* make TRUE (1 = signal) so no more children start */
   kill(0,SIGTERM);             /* signal all processes in our process group */
   }
/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
/* This routine takes a string, parses the words out of it, and puts them
 * in consecutive members of the array pointed to by pointer.  
 *        ***  string IS MODIFIED by having NULL's put in it  ***
 *
 * NOTE: This routine does not verify terminating " or ' if string ends.  
 *       Only BLANK is used to separate words, not TAB, VTAB, etc. 
 *       There is no escape mechanism such as \" or \' for strings .
 *       There is no wild card substitution.
 *       (Use /bin/sh with a command string if you need all that stuff)
 */
static int parse_command(
   char *string,            /* the string to parse, which WILL BE MODIFIED */
   char *pointer[],    /* the array of pointers to point to each parameter */
   int limit) {  /* maximum members in the array, NOT counting ending NULL */

   char tc;    /* terminating character of parse, if parsing quoted string */
   int j;                            /* index into the array of parameters */
   char *p; 
   char temp[256];

   for(j=0, p=string; *p; j++) {                /* for each char in string */
      while(*p && *p == ' ') p++;                   /* skip leading blanks */
      if(!*p) break;                           /* nothing after the blanks */
      if(j == limit) {       /* something to parse but no room in storage? */
         pointer[limit] = NULL;   /* correctly terminate what we did store */
         sprintf(temp,
         "ERROR:  parse_command() parameter limit of %d exceeded.",limit);
         log_entry(temp);
         return 1;
         }
      tc=' ';                 /* normally terminating character is a BLANK */
      if(*p == '"') { tc='"' ; p++; }   /* set terminating char if using " */
      if(*p == '\''){ tc='\''; p++; }   /* set terminating char if using ' */
      pointer[j]=p;           /* set array to point to beginning of string */
      while(*p && *p != tc) p++;                     /* move to next blank */
      if(!*p) continue;            /* end of parameter AND string to parse */
      *p=0;                                  /* terminate the parameter */
      p++;    /* skip over blank we just NULL'd to terminate the parameter */
      }
   pointer[j]=0;              /* terminate the list with a NULL pointer */
   return 0;
   }


/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
/* Store the exit code in the array, and look up the pid to find the     */
/* index into table of processes, cause we will need the channel we ran  */
/* this process on to know what to run next...                           */

static int get_proc_info(
   pid_t pid, 
   int exit_status,
   int *index) { 

   int i;
   char temp[256];

   for(i=0; i < line_count; i++) {
      if(line[i].pid != pid) continue;  /* not the one we want */
      /* *channel=line[i].channel; */
      line[i].exit_status=exit_status;
      if(WIFSIGNALED(exit_status)) { /* an exit due to uncaught signal */
         sprintf(temp,"FINISHED: %d:%d:%d  PID:%d  <%s> <%s>  SIGNAL:%d",
         line[i].u_step, line[i].channel, line[i].u_process, line[i].pid,
         line[i].desc, line[i].command, WTERMSIG(line[i].exit_status) );
         }
      else { /* a exit due to end of program */
         sprintf(temp,"FINISHED: %d:%d:%d  PID:%d  <%s> <%s>  EXIT:%d",
         line[i].u_step, line[i].channel, line[i].u_process, line[i].pid,
         line[i].desc, line[i].command, WEXITSTATUS(line[i].exit_status) );
         }
      log_entry(temp);

      line[i].pid=0; /* reset line[i].pid to zero, in case the PID is reused */
      *index=i;
      return 0;
      }
   return 1;  /* ERROR: (NOTE: index is not set in this case) */
   }

/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
/* This routine is called to start child processes.  Processes on a 
 * channel are started consecutively, until there are no more to start
 * for a particular channel, so this routine fully expects to be asked
 * to start a process on a channel when there are no more.  Return codes:
 *
 * >0 : index of line in process table that was started
 * -1 : no more processes on the channel to start
 * -2 : parsing error of the command line
 * -3 : fork() failure
 *
 * Enhancement: make it sleep() and retry after a while if fork() fails?
 */
static int start_proc(
   int step,                        /* step number of the program to start */
   int channel,                     /* channel number of the program to start */
   int process,   
   int *index)    {                 /* process number of the program to start */

   static int fork_failure;         /* track number of fork() failures */
   char hold_command[FIELDSIZE];    /* we parse a copy of line.command */
   char *plist[PARMLISTSIZE+1];/* the array of pointers to the parameter list */
         /* like *argv[], need room for NULL pointer that terminates the list */
   pid_t pid;
   char temp[256];
   int i;
 

   for(i=0; i<line_count; i++) { /* check all commands for program to start? */
      if(line[i].step != step) continue;
      if(line[i].channel != channel) continue;
      if(line[i].process != process) continue;
      break;                                            /* we found a match! */
      }
   *index=0;      /* when there is no process start, index never gets set... */
   if(i==line_count) return -1;          /* nothing to start on this channel */
   *index = i; /* we set the index number of the process we found for caller */
   /* build an "argv" like list for the execvp() call */
   strcpy(hold_command,line[i].command);
   if(parse_command(hold_command, plist, PARMLISTSIZE)) {  
      sprintf(temp,"ERROR:  PARSE FAILED: %d:%d:%d  <%s> <%s>",
      line[i].u_step, line[i].channel, line[i].u_process,
      line[i].desc, line[i].command);
      log_entry(temp);
      return -2;    /* parse error shouldn't happen, this is checked earlier */
      }
   fflush(stdout);  /* fork() gets copy of buffer, and it would output twice */
   fork_failure=0;                         /* re-initialize for each program */
   while((pid=fork()) == -1) {         /* loop here so we can try to recover */
      fork_failure++;
      if(fork_failure == MAX_FORKS) {
         sprintf(temp,"ERROR:  fork() FAILURE LIMIT (%d Times): %d:%d:%d  <%s> <%s>",
         fork_failure, line[i].u_step, line[i].channel, line[i].u_process,
         line[i].desc, line[i].command);
         log_entry(temp);
         return -3;                    /* give up */
         }
      sprintf(temp,"NOTICE: fork() FAILED (%d Time): %d:%d:%d  <%s> <%s>",
      fork_failure, line[i].u_step, line[i].channel, line[i].u_process,
      line[i].desc, line[i].command);
      log_entry(temp);
      sleep((unsigned) FORK_DELAY); /*hope the process table gets cleaned up */
      }
   if(pid) {                                                 /* we're parent */
      sprintf(temp,"STARTING: %d:%d:%d  PID:%d  <%s> <%s>",
      line[i].u_step, line[i].channel, line[i].u_process, pid,
      line[i].desc, line[i].command);
      log_entry(temp);
      line[i].pid=pid;        /* store the pid so we can look up the channel */
      return i;                  /* return index into the table of processes */
      }

   /* else we're the child */
   /* NOTE:  parse_command() put a NULL after the first word in hold_command */
   (void) execvp(hold_command,plist); 
   exit(69);     /*  exec() failed, exit with an error code to notify parent */
   /*NOTREACHED*/
   }
/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
/* This process parses the command line from the "ss" file.
 * It return the field number where the error was found.
 * It needs to check parse of the command line, and check for lines that
 * are not consecutive, or start with the wrong number, etc. 
 */
static int parse_line(
   int member,        /* array member to stuff parsed information */
   char *buffer)  {   /* the unparsed line */

   char *p;  /* current place in buffer (the string to parse) */
   char *o;  /* output pointer */
   static int last_u_step = -1;          /* Initialize to "unlikely" value to */
                        /* force first read value to set first .step to SBASE */
   static int current_step = SBASE-1;       /* Tracks sequential step numbers */
   static int current_process[TOTALCHANNELS];/*current proc # for each channel*/
   char temp[256];
   int i;

   p=buffer;                                    /* start at beginning of line */
   line[member].u_step = (int) strtol(p, &p, 10);    /* Field 1 - step number */
   if(*p != '~') return 1; /* did we encounter unexpected invalid characters? */
   /* every time the users step number changes, we increment the "internal"   */
   /* step number by 1.  This is done so start_proc() knows the next step     */
   /* number to go to, and the "users" step numbers need to be in sorted, but */
   /* not in sequential order... */
   if(line[member].u_step != last_u_step )  {
      current_step++;                    /* increment sequential step counter */
      last_u_step=line[member].u_step;        /* re-assign new "current" step */
      /* for any given step, each time there is a new "entry" for a channel,  */
      /* the process number (initially PBASE) is incremented by one */
      for(i=0; i<TOTALCHANNELS; i++)  current_process[i] = PBASE;
      }
   line[member].step=current_step; 

   p++;                                                    /* skip over the ~ */
   line[member].channel = (int) strtol(p, &p, 10);       /* Field 2 - channel */
   if(*p != '~') return 2; /* did we encounter unexpected invalid characters? */
   if(line[member].channel >= TOTALCHANNELS) {             /* Invalid number? */
      sprintf(temp,
      "ERROR:  Script trying to use channel %d. Only 0 - %d Available.",
      line[member].channel,TOTALCHANNELS-1);
      log_entry(temp);
      return 2;
      }

   p++;                                                    /* skip over the ~ */
   line[member].u_process = (int) strtol(p, &p, 10);   /* Field 3 - process # */
   if(*p != '~') return 3; /* did we encounter unexpected invalid characters? */
   if(line[member].u_process < 0) { 
      sprintf(temp, "ERROR:  parse_line() lowest allowed process number is 0");
      log_entry(temp);
      return 3;
      }
   line[member].process=current_process[line[member].channel]; 
   current_process[line[member].channel]++;   /* up process # for the channel */

   p++;                                                    /* skip over the ~ */
   for(o=line[member].desc; *p && *p != '~'; p++, o++) *o=*p; /* copy Field 4 */
   if(*p != '~') return 4;                   /* copy terminated unexpectedly? */
   *o=0;                         /* terminate the string in the line array */

   p++;                                                    /* skip over the ~ */
   for(o=line[member].command; *p && *p != '~';p++,o++)*o=*p; /* copy Field 5 */
   *o=0;                         /* terminate the string in the line array */

   /* NOTE: if "command" ends with a ~ then additional comments can be on end */
   line[member].pid=0;                /* initialize fields that get set later */
   line[member].start=line[member].stop=0;
   line[member].exit_status=0;
   return 0;
   }
/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
/* here we open and read each line from the ss (Stream Script) file, and */
/* parse it into fields.  We then parse the command line field into      */
/* command line parameters, which exec() will use                        */

static int parse_script(
   char *ss_file,
   int delete_ss_file)  {

   char hold_command[FIELDSIZE];
   char buffer[256];
   FILE *sfd;  /* script file descriptor */
   char *plist[PARMLISTSIZE+1]; /* an array of pointers to the parameter list */
   char temp[256];
   int i,j;
   
   if((sfd=fopen(ss_file,"r")) == NULL) {
      sprintf(temp,"ERROR:  fopen() of %s for parsing failed.",ss_file);
      log_entry(temp);
      return 51;
      }

   j=line_count=0; 
   while(fgets(buffer, 256, sfd) != NULL)  {          /* Read lines till EOF */
      j++;                                    /* keep just for error message */
      buffer[strlen(buffer) -1] = 0;             /* NULL over the newline */
      if(*buffer == '#' || *buffer == '\n') {
         if(buffer[1] != '#') continue;       /* comment/blank line, ignore */
         /* "double comment" line has ##, and is logged (i.e.,contains SID) */
         /* we log these to allow verification of original .ss files utilized */
         sprintf(temp,"COMMENT: %s",buffer);
         log_entry(temp);
         continue;    /* comment or blank */
         }
      if(line_count == MAXSCRIPTLINES) {           /* Yikes, too many lines. */
         (void) fclose(sfd);
         sprintf(temp,"ERROR:  Max of %d lines in script file %s exceeded.",
         MAXSCRIPTLINES,ss_file);
         log_entry(temp);
         return 52;
         }
      if(i=parse_line(line_count, buffer)) {    /* parse each input file line */
         sprintf(temp, "ERROR:  parse error in field %d on line %d of %s.",
         i,j,ss_file);
         (void) fclose(sfd);
         log_entry(temp);
         return 2;
         }
      /* parse_command() modifies string, so we us a "copy" for testing parse */
      strcpy(hold_command,line[line_count].command);
      if(parse_command(hold_command, plist, PARMLISTSIZE)) {  
         /* test command line parameter parsing - We don't save results, but */
         /* do this so we can find as many errors as early on as possible    */
         sprintf(temp, "ERROR:  line %d of %s parse of <%s>",
         j,ss_file,line[line_count].command);
         (void) fclose(sfd);
         log_entry(temp);
         return 3;
         }
      line_count++;
      }
   (void) fclose(sfd);
   if(delete_ss_file) unlink(ss_file);  /* command line option -d */
   return 0;
   }

/*************************************************************************/
/* PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE PROCEDURE */
/*************************************************************************/
/* This routine gets the user's full name out of the /etc/passwd file
 * If a "login" is passed, it is looked up, and retunred if nothing
 * found in password file, otherwise the full name is returned.
 * If NULL is passed, getenv() is used to get the login.
 * if getenv() fails, a default string of "NO $LOGNAME" is passed.
 * NOTE: the char * is to static data, and can easily be over written
 *       by another call to this routine, or getpwnanme();
 */
static char *getfullname(char *login) 
   {

   static char noname[16];
   char *p;
   struct passwd *pw;

   if(login==NULL) {
      strcpy(noname,"NO $LOGNAME");
      if((p=getenv("LOGNAME"))==NULL) return noname;  /* LOGNAME not defined */
      }
   else p=login;
   if((pw=getpwnam(p)) == NULL) return p;  /* failed, at least return login */
   return pw->pw_gecos;                    /* user name is in "gecos" field */
   }


/*************************************************************************/
/* MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN */
/*************************************************************************/
int main(int argc, char *argv[]) 
   {

   extern void TAhome_error(const char *, const char *);
   extern int TAshuffle(const char *, const char *, const char *, int);
   extern const char *StreamGetHostName();

   char *reason=NULL;     /* -r parameter, log in logfile */
   char *datagroup=NULL;       /* passed as arg to -f parameter */
   int option_error=0;    /* tracks errors in processing options with getopt */
   int delete_ss_file=0;  /* TRUE if -d option passed, i.e. delete .ss file */
   int first_step;
   int last_step;
   int err_flag=0;   /* set if processing stops prematurely due to error  */
                  /* starting a process, or a bad return code, this is set as */
                  /* return code when "other" channels on current step finish */
   int current_step;
   int channel_finished[TOTALCHANNELS];              /* set to TRUE or FALSE */
   int finished[TOTALCHANNELS]; /* contains finished process # for a channel */
   int current[TOTALCHANNELS];  /* contains current process # for a channel */
   pid_t wait_return; 
   int exit_status;
   int channel;                 /* temp variable to store channel */
//   char main_dir[FIELDSIZE];    /* location of fips script, log files, etc. */
   char log_file[FIELDSIZE];    /* name of the log file */
   char out_file[FIELDSIZE];    /* name of the file stdio & stderr go to */
   char at_filename[128];       /* name of the $TALOG/$FIPS/.stream.at file */
   char ctl_filename[128];      /* control file name */
   char ss_file[FIELDSIZE];     /* stream script file */
   char buffer[FIELDSIZE];      /* for line read from stream lock file */
//   struct flock lock;
   int index;                   /* index into table of processes we built */
   FILE *at_fp;                 /* file pointer to the scheduling at file */
   FILE *ctl_fp;                /* file pointer to the control file */
   int wrote_pause_state=FALSE; /* log this message once... */
   int new_last_step;           /* the users chosen new last step, i.e., 1,2 */
   int u_laststep;              /* users last step, i.e., 100, 200 */
   char *p;
   int i,j;
   long m;
   char temp[256];

   pid_t child_pid;

   progname=argv[0];
   time(&StartTime);      /* get the time this started for duration logging */
   if((child_pid=fork()) == -1) {  /* start a new process that will detatch */
      sprintf(temp,"Initial fork() failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      exit(1);
      }

   if(child_pid) exit(0);  /* we are the parent? exit and run as daemon */

   opterr=0;  /* disable error message from getopt() to stderr */
   while((i=getopt(argc,argv,"ldr:e:f:")) != EOF)  {
      switch(i) {
         case 'd':
            delete_ss_file=1;  /* after parse of script file, delete it */
            break;
         case 'f':
            datagroup=optarg;       /* set datagroup to the command passed datagroup name */
            global_datagroup=datagroup;  /* now need a global for logging routine */
            strcpy(email_datagroup,optarg); /* used in subject line of email mesg.*/
            break;
         case 'e':
            email_to=optarg;
            break;
         case 'r':
            reason=optarg;
            break;
         case 'l':
            TIMS_log=1;  /* do logging for TIMS */
            break;
         case '?':
            option_error++;
            break;
         }
      }

   if(!datagroup) option_error++;               /* no datagroup passed or no -f option */
   if((argc-optind) != 1) option_error++;  /* not exactly one SS_FILE passed */

   if(option_error) {
      sprintf(temp,
      "Usage: %s -f DATAGROUP [-l] [-r reason] [-e EMAIL_TO_NAME] [-d] SS_FILE",progname);
      TAhome_error(progname,temp);
      exit(1);
      }  

   sprintf(ss_file,"%s",argv[optind]);

   BTDataGroupManager dataGroupMgr;
   if (dataGroupMgr.setDataGroup(datagroup)) {
      sprintf(temp,"Datagroup '%s' invalid--%s",datagroup,dataGroupMgr.getErrorString());
      TAhome_error(progname,temp);
      exit(1);
   }

   const char *logPath = dataGroupMgr.getLogPath();
   if (dataGroupMgr.makeDirPath(logPath)) {
      sprintf(temp,"mkdir() of %s failed: %s",logPath,dataGroupMgr.getErrorString());
      TAhome_error(progname,temp);
      exit(1);
   }
   
   /* Just in case there are any "old" control files, we delete
    * them, before starting this stream...
    */
   sprintf(temp,"%s/.stream.ctl",logPath);
   unlink(temp);  /* failure is OK, as we don't expect file to exist */

   /* If this was a scheduled (via "at") run, we need to remove the
    * file with the schedule information...
    */
   sprintf(at_filename,"%s/.%s.at",logPath,dataGroupMgr.getPrincipalGroup());
   if((at_fp=fopen(at_filename,"r")) != NULL)  {
      fgets(temp, sizeof temp, at_fp);  /* get user name */
      fgets(temp, sizeof temp, at_fp);  /* get job number & date */
      fclose(at_fp);
      m=strtol(temp,&p,10);  /* translate file name to "time" scheduled */
      if(m <= time(NULL)) unlink(at_filename); /* old? delete the file */
      }

   /* Delete FIPS9, move FIPS8 to FIPS9, FIPS7 to FIPS8, and so on.  Lastly, move FIPS to FIPS0 */
   if(TAshuffle(logPath, dataGroupMgr.getPrincipalGroup(), "", 9) == -1) {
      //(void) close(lock_fd);
      //unlink(lockfile_name);
      sprintf(temp,"TAshuffle() of %s/%s failed",logPath,dataGroupMgr.getPrincipalGroup());
      TAhome_error(progname,temp);
      exit(1);
      }

   sprintf(log_file,"%s/%s",logPath,"stream.log");

   /* here we delete file 2, move the file 1 to 2, 0 to 1, file to 0 */
   if(TAshuffle(logPath,"stream","log",9) == -1) {
      //(void) close(lock_fd);
      //unlink(lockfile_name);
      sprintf(temp,"TAshuffle() of %s/stream.log failed",logPath);
      TAhome_error(progname,temp);
      exit(1);
      }

   sprintf(log_file,"%s/%s",logPath,"stream.log");
   if((log_fp=fopen(log_file,"a")) == NULL) {  
      //(void) close(lock_fd);  /* this also removes the lock */
      //unlink(lockfile_name);
      sprintf(temp,"fopen() of %s failed: %s.", log_file,strerror(errno));
      TAhome_error(progname,temp);
      exit(1);
      }


   if((p=getenv("LOGNAME")) == NULL) p="<LOGNAME not set!>";
   sprintf(temp,"BEGIN: Program:%s  DATAGROUP:%s  Script:%s  Login:%s  PID:%d",
   progname,datagroup,ss_file,p,getpid());  
   log_entry(temp);


   if(TIMS_log) {
      if(reason) sprintf(temp,"Started. (Reason:  %s)",reason);
      else sprintf(temp,"Started.");
      do_tims_log(0,LGPROSTART,temp);
      }

   /* become new session leader, process group leader, detatch from terminal */
   if(setsid() == -1) {
      sprintf(temp,"ERROR: setsid() failed: %s.",strerror(errno));
      done(temp,11);
      }

   /* handle SIGTERM, from manually generated kill(1) or "top" program. */
   if(sigaction(SIGTERM,NULL,&term_act)) {  /* fill in structure fields */
      sprintf(temp,"ERROR: sigaction() to get default failed: %s", 
      strerror(errno));
      done(temp,31);
      }
   term_act.sa_handler=sig_handler;    /* set the signal handler function */
   if(sigaction(SIGTERM,&term_act,NULL) == -1) { /* and set the new action */
      sprintf(temp,"ERROR: sigaction() to reset handler failed: %s", 
      strerror(errno));
      done(temp,32);
      }

   /* here we delete file 2, move the file 1 to 2, 0 to 1, file to 0 */
   if(TAshuffle(logPath,"stream","out",9) == -1) {
      sprintf(temp,"ERROR: TAshuffle() of %s/stream.out failed",logPath);
      done(temp,14);
      }

   /* close input & re-open stdout & stderr file descriptors to a file descriptor*/
   fflush(stdout);
   fflush(stderr);
   sprintf(out_file,"%s/%s",logPath,"stream.out");

   /* This uses the dup2 method of redirecting stdout/stderr so that the
      output is NOT buffered
  */
   int fd2 = open(out_file,O_RDWR|O_CREAT|O_APPEND,0666);
   if (fd2 == -1) {
      sprintf(temp,"ERROR: open() of %s failed: %s.",out_file,strerror(errno));
      done(temp,18);
   }

   dup2(fd2,STDOUT_FILENO);
   dup2(fd2,STDERR_FILENO);
   if (fd2 > 2) close(fd2);

//
//
//   if((freopen(out_file,"w",stdout)) == NULL) {   /* "w" or "a" or ? */
//      sprintf(temp,"ERROR: freopen() of stdout failed: %s.",strerror(errno));
//      done(temp,18);
//      }
//   if((freopen(out_file,"w",stderr)) == NULL) {   /* "w" or "a" or ? */
//      sprintf(temp,"ERROR: freopen() of stderr failed: %s.",strerror(errno));
//      done(temp,19);
//      }

   if(fclose(stdin) != 0) {
      sprintf(temp,"ERROR: fclose() of stdin failed: %s.",strerror(errno));
      done(temp,20);
      }

   /* Here we will record environment variable settings that may affect
    * the running of programs started by streams.  (Streams itself only
    * depends upon $TALOG, (and $LOGNAME).
    */
   log_entry("ENVIRONMENT DISPLAY:");

   int maxTagLen = 0;
   int dataGroupPartCount = dataGroupMgr.getDataGroupPartCount();
   for (int loop = 0; loop < dataGroupPartCount; loop++) {
       const char *testStr = dataGroupMgr.getDataGroupPartName(loop);
       if (!testStr) testStr = "";
       int thisLen = strlen(testStr);
       if (thisLen > maxTagLen) maxTagLen = thisLen;
   }


   char *padBuffer = new char [maxTagLen+1];
   for (int loop = 0; loop < dataGroupPartCount; loop++) {

     const char *str = dataGroupMgr.getDataGroupPart(loop);
     if (str && str[0]) {
       const char *nameStr = dataGroupMgr.getDataGroupPartName(loop);
       if (!nameStr) nameStr = "";
       padBuffer[0] = 0;
       BTPad(padBuffer,maxTagLen-strlen(nameStr));
       if(fprintf(log_fp," %s: %s<%s>\n",nameStr,padBuffer,str) == -1) {
          sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
          TAhome_error(progname,temp);
        }
     }
   }
   delete [] padBuffer;

   
   if(fprintf(log_fp," TAWORK:     <%s>\n",(p=getenv("TAWORK"))?(p):(""))== -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," TALOG:      <%s>\n",(p=getenv("TALOG"))?(p):(""))== -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," TAINFO:     <%s>\n",(p=getenv("TAINFO"))?(p):(""))== -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," TACONFIG:   <%s>\n",(p=getenv("TACONFIG"))?(p):(""))== -1){
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," STREAMPATH: <%s>\n",(p=getenv("STREAMPATH"))?(p):(""))== -1){
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," TAGEOPATH:  <%s>\n",(p=getenv("TAGEOPATH"))?(p):(""))== -1){
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," TAMODE:     <%s>\n",(p=getenv("TAMODE"))?(p):(""))== -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(p=getenv("TACLEANMODE")) {  /* Per Brian's request, only display if set */
      if(fprintf(log_fp," TACLEANMODE:<%s>\n",p)== -1) {
         sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
         TAhome_error(progname,temp);
         }
      }
   if(fprintf(log_fp," WorkPath:   <%s>\n",dataGroupMgr.getWorkPath()) == -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," ConfigPath: <%s>\n",dataGroupMgr.getConfigPath()) == -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," LogPath:    <%s>\n",dataGroupMgr.getLogPath()) == -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }
   if(fprintf(log_fp," InfoPath:   <%s>\n",dataGroupMgr.getInfoPath()) == -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }

   if(fprintf(log_fp," Hostname:   <%s>\n",StreamGetHostName()) == -1) {
      sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
      TAhome_error(progname,temp);
      }


   /* parse script & verify it as much as possible in the beginning! */
   /* Any error in parse_script will log an error message. We can just exit */
   if(i=parse_script(ss_file,delete_ss_file)) 
      done("ERROR: Stream Script Parse Error. See Log File For Details.",i);   

   /* Here we will put the script in the stream.log file (except comments)
    * (Put a date stamp on this multi line entry, then make entries locally)
    */
   log_entry("SCRIPT LISTING:"); 
   for(i=0; i < line_count; i++) {
      if((fprintf(log_fp,
      "Index %d, Step: %d(%d) Channel: %d  Proc: %d(%d)\n Desc: <%s>\n Command: <%s>\n",
      i, line[i].u_step, line[i].step, line[i].channel, line[i].u_process, 
      line[i].process, line[i].desc, line[i].command)) == -1) {
         sprintf(temp,"fprintf() to log file failed. %s", strerror(errno));
         TAhome_error(progname,temp);
         }
      }

   sprintf(temp,"STATE: RUNNING");
   log_entry(temp);

   /* INITIALIZE DATA */
   first_step=line[0].step;
   last_step=line[line_count-1].step;
   for(i=0; i < TOTALCHANNELS; i++) {
      current[i] = (PBASE-1);    /* so the new process we start will be PBASE */
      finished[i]=current[i];    /* so we will start a new process on channel */
      channel_finished[i]=FALSE;
      }
   current_step=first_step;           /* so we will start with the first step */
   
   sprintf(ctl_filename,"%s/.stream.ctl",logPath);

   /* THIS IS THE MAIN PROCESSING LOOP */
   for(;;) {     /* stay in loop until last_step is executed, and call done() */

      /* HERE we check for a "ctrl" file to control stream, as it runs... */
      if((ctl_fp=fopen(ctl_filename,"r")) != NULL)  {
         if(fgets(buffer, sizeof buffer, ctl_fp) == NULL) {
            fclose(ctl_fp);
            /* we were able to open file before anything written to it */
            /* go on our way, and we'll read it next time */
            }
         else {
            fclose(ctl_fp);
            unlink(ctl_filename);
            buffer[strlen(buffer) -1] = 0;  /* get rid of newline */
            if(start_no_more == 1) { 
               /* we just got a KILL signal, and the contents of the .ctl
                * file are now irrelevant, since we stop NOW for a signal.
                */
               sprintf(temp, 
               "NOTICE: Got Signal - Ignoring ctl command: %s",buffer);
               log_entry(temp);
               }

            else if(!strncmp(buffer,"NEWLASTSTEP:",12)) {
               u_laststep=atoi(&buffer[12]);            /* 100, 200, etc.   */
               /* we translate the users last step to the "real" last step
                * by finding the greatest "real" step > or = to the request
                * i.e., find the greatest "100" step number that is still under
                * the "1" step numbers, and set last_stop to that number  
                */
               new_last_step = SBASE - 1;
               /* default to the lower than lowest POSSIBLE step number
                * The default passed by "control" in stream-mgr possibly
                * should be -1, but we don't use any steps that start with 
                * a step number of 0, so zero works find to "end" a stream.
                */
               for(i=0; i < line_count; i++) {   /* check all line struc's */
                  if(line[i].u_step <= u_laststep) new_last_step=line[i].step;
                  else break;
                  }
               if(new_last_step == last_step) {
                  sprintf(temp, 
                  "NOTICE: Changing Last Step to %d has no affect:",u_laststep);
                  log_entry(temp);
                  }
               else {
                  last_step=new_last_step;
                  sprintf(temp, "NOTICE: New Last Step Changed to %d(%d):",
                  u_laststep, new_last_step);
                  log_entry(temp);
                  if(last_step < current_step) start_no_more=3; 
                  err_flag=3;  /* When done, report "last step" was changed */
                  }

               }
            else if(!strncmp(buffer,"PAUSE",5)) {
               start_no_more=2;
               sprintf(temp, 
               "NOTICE: Will PAUSE When Current Process(es) Finish");
               log_entry(temp);
               }
            else if(!strncmp(buffer,"RESUME",6)) {
               start_no_more=FALSE;
               sprintf(temp,"STATE: RUNNING");
               log_entry(temp);
               wrote_pause_state=FALSE;
               }
            else {
               sprintf(temp, "NOTICE: Unrecognized ctl command: %s",buffer);
               log_entry(temp);
               }
            }
         }

      for(i=0; i<TOTALCHANNELS; i++) {       /* call routine to fork and exec */
         if(start_no_more == 1 || start_no_more == 2) break;  
         /* either signalled, or need to pause. Not reset last stop */
         if(channel_finished[i] == TRUE) continue; /* no more on this channel */
         if(current[i] != finished[i]) continue;   /* process already running */
         if(start_no_more == 3) {   /* don't try to start any more on channel */
            channel_finished[i] = TRUE;          /* as we want to stop now... */
            continue;
            }
         j=start_proc(current_step,i,current[i]+1,&index); /* fork() & exec() */
         if( j < 0) {                            /* could not start a process */
            if(j == -1) channel_finished[i]=TRUE;      /* an expected "error" */
            else {                              /* process could not start... */
               sprintf(temp,
               "NOTICE: Could not start %d:%d:%d.  Setting last step to %d.",
               line[index].u_step, line[index].channel, line[index].u_process,
               line[index].u_step);
               log_entry(temp);
               last_step=current_step;    /*set to end when this step is done */
               /*       but let other parallel steps on other channels finish */
               channel_finished[i]=TRUE;        /* do no more on this channel */
               err_flag=1;          /* when current step done, we will exit*/
               }
            }
         else current[i]++;            /* started a proc.  update proc number */
         }
      for(i=0; i<TOTALCHANNELS; i++) {     /* check if all channels finished? */
         if(channel_finished[i] != TRUE) break;  /* a channel is not finished */
         }
      if(i == TOTALCHANNELS) { /* loop above determined all channels finished */
         current_step++;                          /* move up to the next step */
         if(current_step>last_step) {
            if(err_flag==FALSE) done("Completed Normally",0); 
            else if(err_flag==1) done("Could not start all programs.",4);
            else if(err_flag==2) done("One or more programs exited abnormally.",1);
            else if(err_flag==3) done("Received Request to Stop Prematurely",1);
            else done("Unexpected err_flag value!", 1);
            }
         for(i=0; i<TOTALCHANNELS; i++) {          /* reset to begin next ... */
            finished[i]=current[i]=(PBASE-1); /* processes with the base proc */
            channel_finished[i]=FALSE;    /* and check ALL channels initially */
            }
         continue;           /* start processes on all channels for this step */
         }

      /* ...WHERE WE WAIT FOR CHILDREN TO FINISH */
      wait_return=wait(&exit_status);
      /* wait_return should never be zero - only waitpid() returns zero */
      if(wait_return <= 0)  {   /* error, otherwise returns PID of process */
         if(errno == EINTR) continue;   /* signal handler handles interrupt */
         /* Every child should exit with exit_status set, which will prevent */
         /* running more programs on each channel. This prog. will then exit */
         /* because all channels will be set as finished, and current_step   */
         /* will increment to exceed the last_step */
         if(start_no_more == 1) { 
            /* here due to signal received when no children were running */
            done("Received signal() between running of processes.",5);
            }
         if(start_no_more == 2) {  /* user wants to pause stream */
            if(!wrote_pause_state) {
               sprintf(temp,"STATE: PAUSED");
               log_entry(temp);
               wrote_pause_state=TRUE;  /* only log this ONCE */
               }
            sleep(PAUSE_DELAY);
            continue;  /* another ctr file? go look, or keep paused */
            }
         sprintf(temp,"ERROR: wait() on child failed: %s.",strerror(errno));
         done(temp, 15);  /* kill children? */
         }
      /* here we log exit info, and retrieve the index of the process we */
      /* were running, and then get the channel proc was running on.     */
      if(get_proc_info(wait_return, exit_status, &index)) {  
         done("ERROR: Got PID from wait() not in my table?!" ,16);
         }
      channel=line[index].channel;        /* translate what channel finished */
      finished[channel]=current[channel];

#ifdef dfdf
      if(start_no_more == 3) { /*user reset last stop. Run no more in channel*/
         if(!exit_status) channel_finished[channel]=TRUE;
         /* otherwise let failed exit code be displayed and stop the channel */
         }
#endif

      if(!exit_status) continue;  /* normal process exit, without error */
      if(last_step != current_step) {
         sprintf(temp,
         "ERROR:  Non-zero return code from PID %d. Setting last step to %d.",
         wait_return, line[index].u_step);
         log_entry(temp);
         last_step=current_step; /* change to now end when this step is done */
         }
      channel_finished[channel]=TRUE;        /* do no more on this channel */
      /*         but let all other parallel steps on other channels finish */
      err_flag=2;            /* when current step is done, we will exit */
      }
   /*NOTREACHED*/
   }

/***************************************************************************/
/* END END END END END END END END END END END END END END END END END END */
/***************************************************************************/
