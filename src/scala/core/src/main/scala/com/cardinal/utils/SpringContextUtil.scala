package com.cardinal.utils

import org.springframework.context.{ApplicationContext, ApplicationContextAware}
import org.springframework.stereotype.Component

@Component
class SpringContextUtil extends ApplicationContextAware {
  override def setApplicationContext(applicationContext: ApplicationContext): Unit = {
    SpringContextUtil.applicationContext = applicationContext
  }
}

object SpringContextUtil {
  private var applicationContext: ApplicationContext = _

  def getBean[T](beanClass: Class[T]): T = {
    applicationContext.getBean(beanClass)
  }

  def getBean[T](beanName: String, beanClass: Class[T]): T = {
    applicationContext.getBean(beanName, beanClass)
  }
}
