package com.cardinal.core
import com.cardinal.utils.VersionUtil
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener

class CardinalSpringApplication(primarySources: Class[_]*) extends SpringApplication(primarySources: _*) {
  // You can override methods or add custom behavior here

  private val logger = LoggerFactory.getLogger(getClass)
  setAdditionalProfiles(Environment.getSpringProfiles(): _*)

  private val cardinalArt: String =
    """

      |                                &&&&X;.         :x$&&&&&&X;
      |                          &&&&Xxx++++xxX$X+:.:::.;X+:.:$: . .;x.
      |                      &&&&&$$$$$$$X$X:.;x++;:    :x xxxx$X;     :x
      |                    &&$X+;;;;;;;x&::X+:..  .......+ :&xx+X&&&$Xx+.X
      |                  &&$X++++;;;;+&&+.x++:....   ...+XX&$X+:...:+X&&&&&&&&$X
      |                &&&&$$&$x+++;x&$x$+ +;;;:. ;x$&&$x;:.;$&$$XXXXXxxx$&&&&&&
      |               &&$X$&++;x&$;+$&$$X+$&&&&&&&&x;:.x&&XXXx+++++++++;;..X&&
      |              &&$XX++++:.x&&X++$&&&$$&&&X+;X&&&&$x+++++++++++++++$.   &&&
      |              &&XXXx+x$&&: +xx$&&$&&&&&&x.....    .:$X++++++++++;       $$
      |              &&XXX$&$xxX&$X&Xx++&$X$&...       .::.  .X++++++++;X&:  x  &$
      |              &&$&&$XxX$&&Xx+;;;+&XxX..      x&&&&    x ;++++++XX$&&&&&  $&
      |              &&$XX$$&&Xx+++++++.XXx+.       &&&&&&&&&&..+++X$;.....    .+$$X
      |              &&$$&&$Xxx++xx+;;+; &xx .      .$&&&&&&&. :++$$:.....::;;+xX&&&&
      |                &&&$XXXX$xxxxx+++.:$x+;                ++++&X::X&&&&&$;.&+  &&
      |                  &&$XXXX$$XXX++++.XX+++++++;.    .;++++++++&$+:.:X;;;$;XX:. $
      |                    &&$XX$X$X  $$++ X   +&&X++++++++++++++++++x&&&&$XXXX&X+. +&
      |                      &&&$&$$x; X$++.&;:::.  $$++++++++++++++++++++++++++++: ;&
      |                       &&$X&XX$XX$++:.$;+++:: .&x++++++++++++++++++++++++++. x&
      |                       &&&XX$$&&$x+++.X$++++;: +$+++++++++++++++++++++++++:  &
      |                    &&&$X&&Xxxx++++++x&$+++++; +$+++xxxxxxxxxxxxxxxx+++x+; .&&
      |                 &&&$$$$$$$&$$XxxxxX$&Xxxx+++..&x++xxxxxxxxxxxxxxxxxxxx+: :&
      |             &&&&&$$xxxXXXX$$X$$$$$XXxxxxxx+.x&+++xxxxxxxxxxxxxxxxxxxx; .$&
      |           &&&&$XXXX$$$&$xxxxxxxX$$xxxxxX+;&&xxxxxxxxxxxxxxxxxxxxxxx. ;&&
      |                &&&&&&&&&&&&&&$xxxXXX$&&&$XXxxxxxxxxxxxxxxxxxXXxx.:X&&&
      |           &&&&$XXXX$&&&&$XXXX$$$$$$XXXXXXXXXxxxxxxxXXXXXXXXXXXX&&&&
      |      &&&&XxxxxX$&&$$$&$&&&&&&$XXXXXXXxxx+++X$XXXXXXXXXXXX$&&&&&
      |  &&&XxxxxxX$&$XX$&$$$&&&    &&&&&&&$XXxx+++&&$$$$$&&&&&&&&&
      |&&XxXXX$&&$XX$&$$$&&&               &&&&$&&&&&&&     &&&+$&
      |&&&&&&&&$$&&&&&&&                     & &x;&& &&&&&&&&&&&++&$XXXx++x$&&
      |                                   &$+++xxX:+:.:;;...X&$$$$XX+;:.:&x..&&
      |                                    &&&&&&&&&$x;:.$&+;X&      &&&&&&&&&
      |                                               &&&&
      |""".stripMargin

  // Register a listener to print the version when the application is ready
  addListeners(new ApplicationListener[ApplicationReadyEvent] {
    override def onApplicationEvent(event: ApplicationReadyEvent): Unit = {
      val version = VersionUtil.getVersion
      logger.info("{}Cardinal version: {}", cardinalArt, version)
    }
  })
}
