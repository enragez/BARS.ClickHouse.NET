﻿namespace BARS.ClickHouse.Ado.Impl.Settings
{
    using System;

    internal class StringSettingValue : SettingValue
    {
        public StringSettingValue(string value)
        {
            Value = value;
        }

        public string Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter)
        {
            formatter.WriteString(Value);
        }


        internal override T As<T>()
        {
            if (typeof(T) != typeof(string))
            {
                throw new InvalidCastException();
            }

            return (T) (object) Value;
        }

        internal override object AsValue()
        {
            return Value;
        }
    }
}