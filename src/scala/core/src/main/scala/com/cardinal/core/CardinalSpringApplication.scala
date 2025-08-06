/*
 * Copyright (C) 2025 CardinalHQ, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.cardinal.core
import com.cardinal.utils.VersionUtil
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener

class CardinalSpringApplication(primarySources: Class[_]*) extends SpringApplication(primarySources: _*) {
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
      //logger.info("{}Cardinal version: {}", cardinalArt, version)
      logger.info("Cardinal version: {}", version)

      val keys = System.getenv().keySet().toArray.sortBy(_.toString)
      logger.info("{}", keys.mkString(", "))
    }
  })
}
